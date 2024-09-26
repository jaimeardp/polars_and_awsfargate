import boto3
import json

# Initialize the ECS client
ecs_client = boto3.client('ecs', region_name='us-east-1')


def get_latest_task_definition_revision(task_definition_family):

    response = ecs_client.list_task_definitions(familyPrefix=task_definition_family, sort="DESC", maxResults=1)

    latest_revision = response["taskDefinitionArns"][0]
    latest_revision = latest_revision.split(":")[-1]
    return latest_revision

revision_number = get_latest_task_definition_revision("MyEcsClusterStackMyTaskDef201D8F20")

# Parameters for running the task
cluster_name = "MyEcsClusterStack-MyCluster4C1BA579-fD835oLFB7WQ"
task_definition = f"arn:aws:ecs:us-east-1:<accountn_aws_id>:task-definition/MyEcsClusterStackMyTaskDef201D8F20:{revision_number}"  # e.g., my-task-def:1
subnets = ['subnet-<subnet_id>']  # Your VPC subnets
security_groups = ['sg-<security_group_id>']  # Your security groups

# Optionally, you can define overrides (e.g., environment variables)
overrides = {
    'containerOverrides': [
        {
            'name': 'MyContainer',
            'environment': [
                {
                    'name': 'stock_prices_filename',
                    'value': 'stock_prices_10M.csv'
                },
                {
                    'name': 'trading_volume_filename',
                    'value': 'trading_volume_10M.csv'
                },
                {
                    'name': 'financial_filename_enriched',
                    'value': 'stock_trading_volume_10M.parquet'
                }
            ]
        }
    ]
}

# Run the ECS Fargate task
response = ecs_client.run_task(
    cluster=cluster_name,
    taskDefinition=task_definition,
    launchType='FARGATE',
    count=1,
    platformVersion='LATEST',
    networkConfiguration={
        'awsvpcConfiguration': {
            'subnets': subnets,
            'securityGroups': security_groups,
            'assignPublicIp': 'ENABLED'  # Or 'DISABLED' if not needed
        }
    },
    overrides=overrides
)

# Print the response from ECS
print(json.dumps(response, indent=4, default=str))
