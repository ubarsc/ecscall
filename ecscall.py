"""
Simple wrapper for spreading a series of function calls across an
AWS ECS cluster

Example usage:

    import ecscall

    def myFunc(a, b):
        "Do some big calculation"
        val = a + b
        return val

    argTupleList = [
        (1, 2),
        (3, 4),
        (5, 6),
        (7, 8),
        (9, 10)
    ]
    ecsClusterParams = ecscall.makeEcsClusterParams_Fargate(jobName='MyJob',
        # .... The rest of the AWS config arguments
        )
    numWorkers = 3
    retDict = ecscall.callFunc(myFunc, argTupleList, numWorkers,
        ecsClusterParams)
    for ndx in sorted(retDict.keys()):
        args = argTupleList[ndx]
        value = retDict[ndx]
        print(args, value)

This will run the function myFunc on all the pairs given in argTupleList,
across 3 workers, and return a dictionary with a key for each index value,
and the corresponding function return value.

Timeout values can be changed from their defaults using the optional callCfg
argument.

"""
import sys
import argparse
from concurrent import futures
from multiprocessing.managers import BaseManager
import queue
import threading
import secrets
import socket
import time
import random
import traceback

import boto3
import cloudpickle


__version__ = "1.0.0"


def callFunc(userFunc, argTupleList, numWorkers, ecsClusterParams,
        callCfg=None):
    """
    Call the given user function repeatedly, with arguments
    from argTupleList. Workers are running calls concurrently
    on an ECS cluster.

    Parameters
    ----------
      userFunc : callable
        The Python function to call
      argTupleList : list of tuples
        List of argument tuples. Each list element is a tuple
        of arguments for userFunc, i.e. userFunc is called
        as userFunc(*argTuple).
      numWorkers : int
        Number of concurrent workers to run
      ecsClusterParams : dict
        Dictionary of parameters to various boto3 calls to manage
        the ECS cluster.
      callCfg : EcsCallCfg or None
        If given, this allows over-ride of default behaviour of ecscall.
        Currently this consists of various timeout values.

    Returns
    -------
      returnValDict : dict
        Dictionary of userFunc return values. Key is the index number
        corresponding to the order in argTupleList. If the call for
        a particular arg tuple did not return (i.e. had an error),
        that index value will be missing from the dictionary.

    """
    if callCfg is None:
        callCfg = EcsCallCfg()

    try:
        clusterMgr = _EcsClusterMgr(userFunc, argTupleList, numWorkers,
            ecsClusterParams, callCfg)
        clusterMgr.startWorkers()
        returnValsDict = clusterMgr.processReturnVals()
    finally:
        clusterMgr.shutdown()

    return returnValsDict


def makeEcsClusterParams_Fargate(jobName=None, containerImage=None,
            taskRoleArn=None, executionRoleArn=None,
            subnet=None, securityGroups=None, cpu='0.5 vCPU', memory='1GB',
            cpuArchitecture=None, cloudwatchLogGroup=None, tags=None):
    """
    Helper function to construct a minimal ecsClusterParams dictionary
    suitable for using ECS with Fargate launchType, given just the
    bare essential information.

    Returns a Python dictionary.

    jobName : str
        Arbitrary string, optional. If given, this name will be incorporated
        into some AWS/ECS names for the compute workers, including the
        container name and the task family name.
    containerImage : str
        Required. URI of the container image to use for compute workers. This
        container must have ecscall installed. It can be the same container as
        used for the main script, as the entry point is over-written.
    executionRoleArn : str
        Required. ARN for an AWS role. This allows ECS to use AWS services on
        your behalf. A good start is a role including
        AmazonECSTaskExecutionRolePolicy, which allows access to ECR
        container registries and CloudWatch logs.
    taskRoleArn : str
        Required. ARN for an AWS role. This allows your code to use AWS
        services. This role should include policies such as AmazonS3FullAccess,
        covering any AWS services your compute workers will need.
    subnet : str
        Required. Subnet ID string associated with the VPC in which
        workers will run.
    securityGroups : list of str
        Required. List of security group IDs associated with the VPC.
    cpu : str
        Number of CPU units requested for each compute worker, expressed in
        AWS's own units. For example, '0.5 vCPU', or '1024' (which
        corresponds to the same thing). Both must be strings. This helps
        Fargate to select a suitable VM instance type (see below).
    memory : str
        Amount of memory requested for each compute worker, expressed in MiB,
        or with a units suffix. For example, '1024' or its equivalent '1GB'.
        This helps Fargate to select a suitable VM instance type (see below).
    cpuArchitecture : str
        If given, selects the CPU architecture of the hosts to run worker on.
        Can be 'ARM64', defaults to 'X86_64'.
    cloudwatchLogGroup : str or None
        Optional. Name of CloudWatch log group. If not None, each worker
        sends a log stream of its stdout & stderr to this log group. The
        group should already exist. If None, no CloudWatch logging is done.
        Intended for tracking obscure problems, rather than to use permanently.
    tags: dict or None
        Optional. If specified this needs to be a dictionary of key/value
        pairs which will be turned into AWS tags. These will be added to
        the ECS cluster, task definition and tasks. The keys and values
        must all be strings. Requires ``ecs:TagResource`` permission.

    Only certain combinations of cpu and memory are allowed, as these are used
    by Fargate to select a suitable VM instance type. See ECS.Client.run_task()
    documentation for further details.

    """
    jobIDstr = _makeJobIDstr(jobName)
    containerName = 'ECSCALL_{}_container'.format(jobIDstr)
    taskFamily = "ECSCALL_{}_task".format(jobIDstr)
    clusterName = "ECSCALL_{}_cluster".format(jobIDstr)
    aws_tags = None
    if tags is not None:
        # covert to AWS format
        aws_tags = []
        for key, value in tags.items():
            obj = {'key': key, 'value': value}
            aws_tags.append(obj)

    createClusterParams = {"clusterName": clusterName,
        'tags': [{'key': 'ECSCALL-cluster', 'value': ''}]}
    if aws_tags is not None:
        createClusterParams['tags'].extend(aws_tags)

    containerDefs = [{'name': containerName,
                      'image': containerImage,
                      'entryPoint': ['/usr/bin/env', 'ecscall_worker']}]
    if cloudwatchLogGroup is not None:
        # We are using the default session, so ask it what region
        session = boto3._get_default_session()
        regionName = session.region_name
        # Set up the cloudwatch log configuration
        containerDefs[0]['logConfiguration'] = {
            'logDriver': 'awslogs',
            'options': {
                'awslogs-group': cloudwatchLogGroup,
                'awslogs-stream-prefix': f'/ECSCALL_{jobIDstr}',
                'awslogs-region': regionName
            }
        }

    networkConf = {
        'awsvpcConfiguration': {
            'assignPublicIp': 'DISABLED',
            'subnets': [subnet],
            'securityGroups': securityGroups
        }
    }

    taskDefParams = {
        'family': taskFamily,
        'networkMode': 'awsvpc',
        'requiresCompatibilities': ['FARGATE'],
        'containerDefinitions': containerDefs,
        'cpu': cpu,
        'memory': memory
    }
    if taskRoleArn is not None:
        taskDefParams['taskRoleArn'] = taskRoleArn
    if executionRoleArn is not None:
        taskDefParams['executionRoleArn'] = executionRoleArn
    if cpuArchitecture is not None:
        taskDefParams['runtimePlatform'] = {'cpuArchitecture': cpuArchitecture}
    if aws_tags is not None:
        taskDefParams['tags'] = aws_tags

    runTaskParams = {
        'launchType': 'FARGATE',
        'cluster': clusterName,
        'networkConfiguration': networkConf,
        'taskDefinition': 'Dummy, to be over-written within ecscall',
        'overrides': {'containerOverrides': [{
            "command": 'Dummy, to be over-written within ecscall',
            'name': containerName}]}
    }
    if aws_tags is not None:
        runTaskParams['tags'] = aws_tags

    ecsClusterParams = {
        'register_task_definition': taskDefParams,
        'create_cluster': createClusterParams,
        'run_task': runTaskParams
    }
    return ecsClusterParams


def makeEcsClusterParams_PrivateCluster(jobName=None, numInstances=None,
            ami=None, instanceType=None, containerImage=None,
            taskRoleArn=None, executionRoleArn=None,
            subnet=None, securityGroups=None, instanceProfileArn=None,
            memoryReservation=1024, cloudwatchLogGroup=None, tags=None):
    """
    Helper function to construct a basic ecsClusterParams dictionary
    suitable for using ECS with a private per-job cluster of EC2 instances,
    given just the bare essential information.

    Returns a Python dictionary.

    jobName : str
        Arbitrary string, optional. If given, this name will be incorporated
        into some AWS/ECS names for the compute workers, including the
        container name and the task family name.
    numInstances : int
        Number of VM instances which will comprise the private ECS cluster.
        The ecscall compute workers will be distributed across these, so it
        makes sense to have the same number of instances, i.e. one worker
        on each instance.
    ami : str
        Amazon Machine Image ID string. This should be for an ECS-Optimized
        machine image, either as supplied by AWS, or custom-built, but it must
        have the ECS Agent installed. An example would
        be "ami-00065bb22bcbffde0", which is an AWS-supplied ECS-Optimized
        image.
    instanceType : str
        The string identifying the instance type for the VM instances which
        will make up the ECS cluster. An example would be "a1.medium".
    containerImage : str
        Required. URI of the container image to use for compute workers. This
        container must have ecscall installed. It can be the same container as
        used for the main script, as the entry point is over-written.
    executionRoleArn : str
        Required. ARN for an AWS role. This allows ECS to use AWS services on
        your behalf. A good start is a role including
        AmazonECSTaskExecutionRolePolicy, which allows access to ECR
        container registries and CloudWatch logs.
    taskRoleArn : str
        Required. ARN for an AWS role. This allows your code to use AWS
        services. This role should include policies such as AmazonS3FullAccess,
        covering any AWS services your compute workers will need.
    subnet : str
        Required. A subnet ID string associated with the VPC in which
        workers will run.
    securityGroups : list of str
        Required. List of security group IDs associated with the VPC.
    instanceProfileArn : str
        The IamInstanceProfile ARN to use for the VM instances. This should
        include AmazonEC2ContainerServiceforEC2Role policy, which allows the
        instances to be part of an ECS cluster.
    memoryReservation : int
        Optional. Memory (in MiB) reserved for the containers in each
        compute worker. This should be small enough to fit well inside the
        memory of the VM on which it is running. Often best to leave this
        as default until out-of-memory errors occur, then increase.
    cloudwatchLogGroup : str or None
        Optional. Name of CloudWatch log group. If not None, each worker
        sends a log stream of its stdout & stderr to this log group. The
        group should already exist. If None, no CloudWatch logging is done.
        Intended for tracking obscure problems, rather than to use permanently.
    tags: dict or None
        Optional. If specified this needs to be a dictionary of key/value
        pairs which will be turned into AWS tags. These will be added to
        the ECS cluster, task definition and tasks, and the EC2 instances.
        The keys and values must all be strings. Requires ``ecs:TagResource``
        permission.

    """
    jobIDstr = _makeJobIDstr(jobName)
    containerName = 'ECSCALL_{}_container'.format(jobIDstr)
    taskFamily = "ECSCALL_{}_task".format(jobIDstr)
    clusterName = "ECSCALL_{}_cluster".format(jobIDstr)
    aws_tags = None
    if tags is not None:
        # covert to AWS format
        aws_tags = []
        for key, value in tags.items():
            obj = {'key': key, 'value': value}
            aws_tags.append(obj)

    createClusterParams = {"clusterName": clusterName,
        'tags': [{'key': 'ECSCALL-cluster', 'value': ''}]}
    if aws_tags is not None:
        createClusterParams['tags'].extend(aws_tags)
    userData = '\n'.join([
        "#!/bin/bash",
        f"echo ECS_CLUSTER={clusterName} >> /etc/ecs/ecs.config"
    ])

    # Set up ecscall-specific instance tags.
    instanceTags = {
        'ResourceType': 'instance',
        'Tags': [
            {'Key': 'ECSCALL-computeworkerinstance', 'Value': ''},
            {'Key': 'ECSCALL-clustername', 'Value': clusterName}
        ]
    }
    #  If user tags are also given, then add them as well.
    if tags is not None:
        for (key, value) in tags.items():
            obj = {'Key': key, 'Value': value}
            instanceTags['Tags'].append(obj)

    runInstancesParams = {
        "ImageId": ami,
        "InstanceType": instanceType,
        "MaxCount": numInstances,
        "MinCount": 1,
        "SecurityGroupIds": securityGroups,
        "SubnetId": subnet,
        "IamInstanceProfile": {"Arn": instanceProfileArn},
        "UserData": userData,
        "TagSpecifications": [instanceTags]
    }

    containerDefs = [{'name': containerName,
                      'image': containerImage,
                      'memoryReservation': memoryReservation,
                      'entryPoint': ['/usr/bin/env', 'ecscall_worker']}]
    if cloudwatchLogGroup is not None:
        # We are using the default session, so ask it what region
        session = boto3._get_default_session()
        regionName = session.region_name
        # Set up the cloudwatch log configuration
        containerDefs[0]['logConfiguration'] = {
            'logDriver': 'awslogs',
            'options': {
                'awslogs-group': cloudwatchLogGroup,
                'awslogs-stream-prefix': f'/ECSCALL_{jobIDstr}',
                'awslogs-region': regionName
            }
        }

    networkConf = {
        'awsvpcConfiguration': {
            'assignPublicIp': 'DISABLED',
            'subnets': [subnet],
            'securityGroups': securityGroups
        }
    }

    taskDefParams = {
        'family': taskFamily,
        'networkMode': 'awsvpc',
        'requiresCompatibilities': ['EC2'],
        'containerDefinitions': containerDefs
    }
    if taskRoleArn is not None:
        taskDefParams['taskRoleArn'] = taskRoleArn
    if executionRoleArn is not None:
        taskDefParams['executionRoleArn'] = executionRoleArn
    if aws_tags is not None:
        taskDefParams['tags'] = aws_tags

    runTaskParams = {
        'launchType': 'EC2',
        'cluster': clusterName,
        'networkConfiguration': networkConf,
        'taskDefinition': 'Dummy, to be over-written within ecscall',
        'overrides': {'containerOverrides': [{
            "command": 'Dummy, to be over-written within ecscall',
            'name': containerName}]}
    }
    if aws_tags is not None:
        runTaskParams['tags'] = aws_tags

    ecsClusterParams = {
        'create_cluster': createClusterParams,
        'run_instances': runInstancesParams,
        'register_task_definition': taskDefParams,
        'run_task': runTaskParams
    }
    return ecsClusterParams


def _worker():
    """
    The function which is called by the worker command line entry point
    """
    p = argparse.ArgumentParser(
        description="Main script run by each ecscall worker")
    p.add_argument("-i", "--idnum", type=int, help="Worker ID number")
    p.add_argument("--channaddr", help=("Directly specified data channel " +
        "address, as 'hostname,portnum,authkey'."))

    cmdargs = p.parse_args()

    (host, port, authkey) = tuple(cmdargs.channaddr.split(','))
    port = int(port)
    authkey = bytes(authkey, 'utf-8')

    dataChan = _NetworkDataChannel(hostname=host, portnum=port,
                                   authkey=authkey)

    userFunc = dataChan.userFunc
    argsQue = dataChan.argsQue
    returnValQue = dataChan.returnValQue
    exceptionQue = dataChan.exceptionQue
    forceExit = dataChan.forceExit
    workerBarrier = dataChan.workerBarrier
    callCfg = dataChan.callCfg

    # Wait at the barrier, so nothing proceeds until all workers have had
    # a chance to start
    workerBarrier.wait(timeout=callCfg.barrierTimeout)

    finished = False
    try:
        argsObj = argsQue.get(block=False)
    except queue.Empty:
        finished = True
    while not finished and not forceExit.is_set():
        try:
            (ndx, args) = argsObj
            retVal = userFunc(*args)
            returnValQue.put((ndx, retVal))
        except Exception as e:
            # Send a printable version of the exception back to main thread
            workerErr = _WorkerErrorRecord(e, cmdargs.idnum)
            exceptionQue.put(workerErr)

        try:
            argsObj = argsQue.get(block=False)
        except queue.Empty:
            finished = True


def _makeJobIDstr(jobName):
    """
    Make a job ID string to use in various generated names. It is unique to
    this run, and also includes any human-readable information available
    """
    hexStr = random.randbytes(4).hex()
    if jobName is None:
        jobIDstr = hexStr
    else:
        jobIDstr = "{}-{}".format(jobName, hexStr)
    return jobIDstr


class EcsCallCfg:
    """
    Control the behaviour of callFunc
    """
    def __init__(self, barrierTimeout=600, waitClusterInstanceCountTimeout=300,
            returnTimeout=300):
        """
        Parameters
        ----------
          barrierTimeout : int
            Number of seconds to wait for all workers to start
          waitClusterInstanceCount : int
            Number of seconds to wait for expected number of instances
          returnTimeout : int
            Number of seconds to wait for a function return to complete
        """
        self.barrierTimeout = barrierTimeout
        self.waitClusterInstanceCountTimeout = waitClusterInstanceCountTimeout
        self.returnTimeout = returnTimeout


class _EcsClusterMgr:
    """
    Manage the ECS cluster running workers for callFunc
    """
    def __init__(self, userFunc, argTupleList, numWorkers, ecsClusterParams,
            callCfg):
        """
        Parameters
        ----------
            userFunc : function
              The user's function to be called
            argTupleList : list of tuples
              Each element is tuple of arguments for userFunc
            numWorkers : int
              Number of worker processes to run, doing the work of calling the
              function
            ecsClusterParams : dict
              Dictionary of parameters for boto3 configuration. See helper
              functions makeEcsClusterParams_Fargate &
              makeEcsClusterParams_PrivateCluster
            callCfg : EcsCallCfg
              Configuration information for ecscall, mostly timeout values

        """
        self.userFunc = userFunc
        self.argTupleList = argTupleList
        self.numWorkers = numWorkers
        self.ecsClusterParams = ecsClusterParams
        self.callCfg = callCfg

    def startWorkers(self):
        """
        Start the ecscall workers, and send the argTupleList into the queue
        """
        self.argsQue = queue.Queue()
        self.forceExit = threading.Event()
        self.exceptionQue = queue.Queue()
        self.workerBarrier = threading.Barrier(self.numWorkers + 1)
        self.returnValQue = queue.Queue()

        # Put all arg tuples into the argsQue (with their index number)
        for i in range(len(self.argTupleList)):
            self.argsQue.put((i, self.argTupleList[i]))

        # Set up the network communication with workers
        self.dataChan = _NetworkDataChannel(userFunc=self.userFunc,
            argsQue=self.argsQue, returnValQue=self.returnValQue,
            forceExit=self.forceExit, exceptionQue=self.exceptionQue,
            workerBarrier=self.workerBarrier, callCfg=self.callCfg)

        self.createdTaskDef = False
        self.createdCluster = False
        self.createdInstances = False
        self.instanceList = None
        self.taskArnList = None

        ecsClient = boto3.client("ecs")
        self.ecsClient = ecsClient

        # Create ECS cluster (if requested)
        try:
            self.createCluster()
            self.runInstances(self.numWorkers)
        except Exception as e:
            self.shutdownCluster()
            raise e

        # Create the ECS task definition (if requested)
        self.createTaskDef()

        # Now create a task for each compute worker
        runTask_kwArgs = self.ecsClusterParams['run_task']
        runTask_kwArgs['taskDefinition'] = self.taskDefArn
        containerOverrides = runTask_kwArgs['overrides']['containerOverrides'][0]
        if self.createdCluster:
            runTask_kwArgs['cluster'] = self.clusterName

        channAddr = self.dataChan.addressStr()
        self.taskArnList = []
        for workerID in range(self.numWorkers):
            # Construct the command args entry with the current workerID
            workerCmdArgs = ['-i', str(workerID), '--channaddr', channAddr]
            containerOverrides['command'] = workerCmdArgs

            runTaskResponse = ecsClient.run_task(**runTask_kwArgs)
            if len(runTaskResponse['tasks']) > 0:
                taskResp = runTaskResponse['tasks'][0]
                self.taskArnList.append(taskResp['taskArn'])

            failuresList = runTaskResponse['failures']
            if len(failuresList) > 0:
                self.dataChan.shutdown()
                msgList = []
                for failure in failuresList:
                    reason = failure.get('reason', 'UnknownReason')
                    detail = failure.get('detail')
                    msg = "Worker {}: Reason: {}".format(workerID, reason)
                    if detail is not None:
                        msg += "\nDetail: {}".format(detail)
                    msgList.append(msg)
                fullMsg = '\n'.join(msgList)
                raise EcsCallError(fullMsg)

        # Do not proceed until all workers have started
        self.workerBarrier.wait(timeout=self.callCfg.barrierTimeout)

    def processReturnVals(self):
        """
        Wait for return values to come back from the workers, and assemble
        them into a dictionary

        Returns
        -------
          returnValsDict : dict
            Key is index number, value is return for the corresponding element
            of argTupleList
        """
        returnValsDict = {}
        numReturnsExpected = len(self.argTupleList)
        queTimeout = self.callCfg.returnTimeout
        done = False
        while not done:
            done = (len(returnValsDict) == numReturnsExpected)
            if not done:
                timedOut = False
                try:
                    retObj = self.returnValQue.get(timeout=queTimeout)
                    (ndx, retVal) = retObj
                    returnValsDict[ndx] = retVal
                except queue.Empty:
                    timedOut = True

                if not self.exceptionQue.empty():
                    exceptionRecord = self.exceptionQue.get()
                    print(exceptionRecord, file=sys.stderr)
                    msg = "The preceding exception was raised in a worker"
                    raise EcsCallError(msg)
                elif timedOut:
                    msg = ("Timeout waiting for function return. Try " +
                           "increasing returnTimeout (currently {})")
                    msg = msg.format(queTimeout)
                    raise EcsCallError(msg)

        return returnValsDict

    def shutdown(self):
        """
        Shut down the workers
        """
        # The order in which the various parts are shut down is critical.
        # Please do not change this unless you are really sure.
        # It is also important that all of it happen, so please avoid having
        # any exceptions raised from within this routine.
        self.forceExit.set()
        self.waitClusterTasksFinished()
        self.checkTaskErrors()
        if hasattr(self, 'dataChan'):
            self.dataChan.shutdown()

        if self.createdTaskDef:
            self.ecsClient.deregister_task_definition(
                taskDefinition=self.taskDefArn)
        # Shut down the ECS cluster, if one was created.
        self.shutdownCluster()

    def shutdownCluster(self):
        """
        Shut down the ECS cluster, if one has been created
        """
        if self.createdInstances and self.instanceList is not None:
            instIdList = [inst['InstanceId'] for inst in self.instanceList]
            self.ec2client.terminate_instances(InstanceIds=instIdList)
            self.waitClusterInstanceCount(self.clusterName, 0)
        if self.createdCluster:
            self.ecsClient.delete_cluster(cluster=self.clusterName)

    def createCluster(self):
        """
        If requested to do so, create an ECS cluster to run on.
        """
        createCluster_kwArgs = self.ecsClusterParams.get('create_cluster')
        if createCluster_kwArgs is not None:
            self.clusterName = createCluster_kwArgs.get('clusterName')
            self.ecsClient.create_cluster(**createCluster_kwArgs)
            self.createdCluster = True

    def runInstances(self, numWorkers):
        """
        If requested to do so, run the instances required to populate
        the cluster
        """
        runInstances_kwArgs = self.ecsClusterParams.get('run_instances')
        if runInstances_kwArgs is not None:
            self.ec2client = boto3.client('ec2')

            response = self.ec2client.run_instances(**runInstances_kwArgs)
            self.instanceList = response['Instances']
            numInstances = len(self.instanceList)
            self.waitClusterInstanceCount(self.clusterName, numInstances)
            self.createdInstances = True

    def getClusterInstanceCount(self, clusterName):
        """
        Query the given cluster, and return the number of instances it has.
        If the cluster does not exist, return None.
        """
        count = None
        response = self.ecsClient.describe_clusters(clusters=[clusterName])
        if 'clusters' in response:
            for descr in response['clusters']:
                if descr['clusterName'] == clusterName:
                    count = descr['registeredContainerInstancesCount']
        return count

    def getClusterTaskCount(self):
        """
        Query the cluster, and return the number of tasks it has.
        This is the total of running and pending tasks.
        If the cluster does not exist, return None.
        """
        count = None
        clusterName = self.clusterName
        response = self.ecsClient.describe_clusters(clusters=[clusterName])
        if 'clusters' in response:
            for descr in response['clusters']:
                if descr['clusterName'] == clusterName:
                    count = (descr['runningTasksCount'] +
                             descr['pendingTasksCount'])
        return count

    def waitClusterInstanceCount(self, clusterName, endInstanceCount):
        """
        Poll the given cluster until the instanceCount is equal to the
        given endInstanceCount
        """
        instanceCount = self.getClusterInstanceCount(clusterName)
        startTime = time.time()
        timeout = self.callCfg.waitClusterInstanceCountTimeout
        timeExceeded = False

        while ((instanceCount != endInstanceCount) and (not timeExceeded)):
            time.sleep(5)
            instanceCount = self.getClusterInstanceCount(clusterName)
            timeExceeded = (time.time() > (startTime + timeout))

    def waitClusterTasksFinished(self):
        """
        Poll the given cluster until the number of tasks reaches zero
        """
        taskCount = self.getClusterTaskCount()
        startTime = time.time()
        timeout = 600
        timeExceeded = False
        while ((taskCount > 0) and (not timeExceeded)):
            time.sleep(5)
            taskCount = self.getClusterTaskCount()
            timeExceeded = (time.time() > (startTime + timeout))

        # If timeExceeded, then we are somehow in shutdown even though
        # some tasks are still running. In this case, we still want to
        # shut down, so kill off any remaining tasks, so we can still
        # delete the cluster
        if timeExceeded:
            for taskArn in self.taskArnList:
                # We are trying to avoid any exceptions raised from within
                # shutdown, so trap all of them.
                try:
                    self.ecsClient.stop_task(cluster=self.clusterName,
                        task=taskArn, reason="Stopped by shutdown")
                except Exception as e:
                    msg = f"Exception '{e}' raised while stopping ECS task"
                    print(msg, file=sys.stderr)

    def createTaskDef(self):
        """
        If requested to do so, create a task definition for the worker tasks
        """
        taskDef_kwArgs = self.ecsClusterParams.get('register_task_definition')
        if taskDef_kwArgs is not None:
            taskDefResponse = self.ecsClient.register_task_definition(**taskDef_kwArgs)
            self.taskDefArn = taskDefResponse['taskDefinition']['taskDefinitionArn']
            self.createdTaskDef = True

    def checkTaskErrors(self):
        """
        Check for errors in any of the worker tasks, and report to stderr.
        """
        if self.taskArnList is None:
            return

        normalStopCodes = set(['EssentialContainerExited'])
        numTasks = len(self.taskArnList)
        # The describe_tasks call will only take this many at a time, so we
        # have to page through.
        TASKS_PER_PAGE = 100
        i = 0
        failures = []
        exitCodeList = []
        stoppedList = []
        while i < numTasks:
            j = i + TASKS_PER_PAGE
            descr = self.ecsClient.describe_tasks(cluster=self.clusterName,
                tasks=self.taskArnList[i:j])
            failures.extend(descr['failures'])
            for t in descr['tasks']:
                stoppedList.append((t.get('stopCode'), t.get('stoppedReason')))
            # Grab all the container exit codes/reasons. Note that we
            # know we have only one container per task.
            ctrDescrList = [t['containers'][0] for t in descr['tasks']]
            for c in ctrDescrList:
                if 'exitCode' in c:
                    exitCode = c['exitCode']
                    if exitCode != 0:
                        reason = c.get('reason', "UnknownReason")
                        exitCodeList.append((exitCode, reason))
            i = j

        for f in failures:
            print("Failure in ECS task:", f.get('reason'), file=sys.stderr)
            print("    ", f.get('details'), file=sys.stderr)
        for (stopCode, stoppedReason) in stoppedList:
            if stopCode is not None or stoppedReason is not None:
                if stopCode not in normalStopCodes:
                    msg = f"Task stopped: {stopCode}. Reason: {stoppedReason}"
                    print(msg, file=sys.stderr)
        for (exitCode, reason) in exitCodeList:
            if exitCode != 0:
                print("Exit code {} from ECS task container: {}".format(
                    exitCode, reason), file=sys.stderr)


class _NetworkDataChannel:
    """
    A network-visible channel to serve out all the required information to
    a group of ecscall workers.

    The channel has several major attributes.

        userFunc : function
            The user function to call (stored in cloudpickle-ed form)
        argsQue : Queue
            Each element of this queue is a tuple (i, argsTuple), where i is
            the index number and argsTuple is a tuple of arguments for the
            user function
        returnValQue : Queue
            As each call completes, its return value is placed in this queue,
            along with its index, as a tuple (i, returnValue)
        forceExit : Event
            If set, this signals that workers should exit
            as soon as possible
        exceptionQue : Queue
            Any exceptions raised in the worker are caught and put
            into this queue, to be dealt with in the main thread.
        workerBarrier : Barrier
            For the relevant compute worker kinds, all
            workers will wait at this barrier, as will the main thread,
            so that no processing starts until all compute workers are
            ready to work.
        callCfg : EcsCallCfg
            Config object for ecscall

    If the constructor is given these major objects as arguments, then this
    is the server of these objects, and they are served to the network on
    a selected port number. The address of this server is available on the
    instance as hostname, portnum and authkey attributes. The server will
    create its own thread in which to run.

    A client instance can be created by giving the constructor the hostname,
    port number and authkey (obtained from the server object). This will then
    connect to the server, and make available the data attributes as given.

    The server must be shut down correctly, and so the shutdown() method
    should always be called explicitly.

    """
    def __init__(self, userFunc=None, argsQue=None, returnValQue=None,
            forceExit=None, exceptionQue=None, workerBarrier=None,
            callCfg=None, hostname=None, portnum=None, authkey=None):
        class DataChannelMgr(BaseManager):
            pass

        if None not in (userFunc, argsQue, returnValQue):
            self.hostname = socket.gethostname()
            # Authkey is a big long random bytes string. Make one which is
            # also printable ascii.
            self.authkey = secrets.token_hex()

            self.userFunc = cloudpickle.dumps(userFunc)
            self.argsQue = argsQue
            self.returnValQue = returnValQue
            self.forceExit = forceExit
            self.exceptionQue = exceptionQue
            self.workerBarrier = workerBarrier
            self.callCfg = cloudpickle.dumps(callCfg)

            DataChannelMgr.register("get_userfunc",
                callable=lambda: self.userFunc)
            DataChannelMgr.register("get_argsque",
                callable=lambda: self.argsQue)
            DataChannelMgr.register("get_returnvalque",
                callable=lambda: self.returnValQue)
            DataChannelMgr.register("get_forceexit",
                callable=lambda: self.forceExit)
            DataChannelMgr.register("get_exceptionque",
                callable=lambda: self.exceptionQue)
            DataChannelMgr.register("get_workerbarrier",
                callable=lambda: self.workerBarrier)
            DataChannelMgr.register("get_callcfg",
                callable=lambda: self.callCfg)

            self.mgr = DataChannelMgr(address=(self.hostname, 0),
                                     authkey=bytes(self.authkey, 'utf-8'))

            self.server = self.mgr.get_server()
            self.portnum = self.server.address[1]
            self.threadPool = futures.ThreadPoolExecutor(max_workers=1)
            self.serverThread = self.threadPool.submit(
                self.server.serve_forever)
        elif None not in (hostname, portnum, authkey):
            DataChannelMgr.register("get_userfunc")
            DataChannelMgr.register("get_argsque")
            DataChannelMgr.register("get_returnvalque")
            DataChannelMgr.register("get_forceexit")
            DataChannelMgr.register("get_exceptionque")
            DataChannelMgr.register("get_workerbarrier")
            DataChannelMgr.register("get_callcfg")

            self.mgr = DataChannelMgr(address=(hostname, portnum),
                                     authkey=authkey)
            self.hostname = hostname
            self.portnum = portnum
            self.authkey = authkey
            self.mgr.connect()

            # Get the proxy objects.
            self.userFunc = cloudpickle.loads(
                eval(str(self.mgr.get_userfunc())))
            self.argsQue = self.mgr.get_argsque()
            self.returnValQue = self.mgr.get_returnvalque()
            self.forceExit = self.mgr.get_forceexit()
            self.exceptionQue = self.mgr.get_exceptionque()
            self.workerBarrier = self.mgr.get_workerbarrier()
            self.callCfg = cloudpickle.loads(
                eval(str(self.mgr.get_callcfg())))
        else:
            msg = ("Must supply either (userFunc, argsQue, etc.)" +
                   " or ALL of (hostname, portnum and authkey)")
            raise ValueError(msg)

    def shutdown(self):
        """
        Shut down the NetworkDataChannel in the right order. This should always
        be called explicitly by the creator, when it is no longer
        needed. If left to the garbage collector and/or the interpreter
        exit code, things are shut down in the wrong order, and the
        interpreter hangs on exit.

        I have tried __del__, also weakref.finalize and atexit.register,
        and none of them avoid these problems. So, just make sure you
        call shutdown explicitly, in the process which created the
        NetworkDataChannel.

        The client processes don't seem to care, presumably because they
        are not running the server thread. Calling shutdown on the client
        does nothing.

        """
        if hasattr(self, 'server'):
            self.server.stop_event.set()
            if self.workerBarrier is not None:
                self.workerBarrier.abort()
            futures.wait([self.serverThread])
            self.threadPool.shutdown()

    def addressStr(self):
        """
        Return a single string encoding the network address of this channel
        """
        s = "{},{},{}".format(self.hostname, self.portnum, self.authkey)
        return s


class _WorkerErrorRecord:
    """
    Hold a record of an exception raised in a remote worker.
    """
    def __init__(self, exc, workerID=None):
        """
        Parameters
        ----------
          exc : Exception
            The exception which as been raised
          workerID : int
            The ID number of the worker
        """
        self.exc = exc
        self.workerID = workerID
        self.formattedTraceback = traceback.format_exception(exc)

    def __str__(self):
        headLine = "Error in ecscall worker"
        if self.workerID is not None:
            headLine += " {}".format(self.workerID)
        lines = [headLine]
        lines.extend([line.strip('\n') for line in self.formattedTraceback])
        s = '\n'.join(lines) + '\n'
        return s


class EcsCallError(Exception):
    "Exceptions specific to ecscall"
