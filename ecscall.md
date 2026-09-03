# ecscall
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

## Classes
### class EcsCallCfg
      Control the behaviour of callFunc

#### &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; EcsCallCfg.\_\_init\_\_(self, barrierTimeout=600, waitClusterInstanceCountTimeout=300, returnTimeout=300)
          Parameters
          ----------
            barrierTimeout : int
              Number of seconds to wait for all workers to start
            waitClusterInstanceCount : int
              Number of seconds to wait for expected number of instances
            returnTimeout : int
              Number of seconds to wait for a function return to complete

### class EcsCallError(Exception)
      Exceptions specific to ecscall

## Functions
### def callFunc(userFunc, argTupleList, numWorkers, ecsClusterParams, callCfg=None)
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

### def makeEcsClusterParams_Fargate(jobName=None, containerImage=None, taskRoleArn=None, executionRoleArn=None, subnet=None, securityGroups=None, cpu='0.5 vCPU', memory='1GB', cpuArchitecture=None, cloudwatchLogGroup=None, tags=None)
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
            workers will run. Note that AWS charges for traffic between
            availability zones so it is preferable to have this subnet
            in the same AZ as the main script (ideally the same subnet -
            this can be obtained by querying the EC2 Instance Metadata).
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

### def makeEcsClusterParams_PrivateCluster(jobName=None, numInstances=None, ami=None, instanceType=None, containerImage=None, taskRoleArn=None, executionRoleArn=None, subnet=None, securityGroups=None, instanceProfileArn=None, memoryReservation=1024, cloudwatchLogGroup=None, tags=None)
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

