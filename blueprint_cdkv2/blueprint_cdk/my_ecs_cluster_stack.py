from aws_cdk import (
    Stack,
    aws_ec2 as ec2,
    aws_ecs as ecs,
    aws_iam as iam,
    aws_ecr as ecr,
    aws_s3 as s3
)
from constructs import Construct


class MyEcsClusterStack(Stack):

    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        s3_bucket = s3.Bucket(self, "datalake-jaimeardp-20240919")

        # Create a VPC for the ECS cluster
        vpc = ec2.Vpc(self, "MyVpc", max_azs=3)

        fargate_security_group = ec2.SecurityGroup(
            self, "FargateSecurityGroup",
            vpc=vpc,
            description="Allow inbound and outbound traffic for Fargate task" # optional 
        )

        # Allow inbound traffic (HTTP, port 80)
        fargate_security_group.add_ingress_rule(
            peer=ec2.Peer.any_ipv4(),
            connection=ec2.Port.tcp(80),
            description="Allow HTTP inbound traffic"
        )

        # Allow outbound traffic
        fargate_security_group.add_egress_rule(
            peer=ec2.Peer.any_ipv4(),
            connection=ec2.Port.all_traffic(),
            description="Allow all outbound traffic"
        )


        # Create an ECR repository
        ecr_repository = ecr.Repository(self, "PipelinePolarsRepGenerator",
            repository_name="pipeline-polars-rep-generator",
            
        )

        # Create an ECS Cluster
        cluster = ecs.Cluster(self, "MyCluster", vpc=vpc)

        execution_role = iam.Role(
            self, "FargateTaskExecutionRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonECSTaskExecutionRolePolicy")
            ]
        )

        # Add permission to the execution role to pull from the ECR repository
        ecr_repository.grant_pull(execution_role)

        task_role = iam.Role(
            self, "FargateTaskRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com")
        )

        # Add policies for accessing AWS resources
        task_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "secretsmanager:GetSecretValue",
                    "s3:PutObject",
                    "s3:GetObject",
                    "s3:ListBucket"
                ],
                resources=["*"]
            )
        )


        # Create a Fargate task definition
        task_definition = ecs.FargateTaskDefinition(
            self, "MyTaskDef",
            execution_role=execution_role,  # Assign execution role
            task_role=task_role, 
                    # Assign task role
            memory_limit_mib=8192 ,
            cpu=1024
        )

        # Add a container to the task definition
        container = task_definition.add_container("MyContainer",
            image=ecs.ContainerImage.from_registry("<account_aws_id>.dkr.ecr.us-east-1.amazonaws.com/pipeline-polars-rep-generator:latest"),
            logging=ecs.LogDrivers.aws_logs(stream_prefix="MyContainerLogs"),
            environment={  # Add environment variables here
                "BUCKET_NAME": s3_bucket.bucket_name  # Pass the S3 bucket name as an environment variable
            }
        )
