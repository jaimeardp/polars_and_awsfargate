#!/usr/bin/env python3
import os

import aws_cdk as cdk

from blueprint_cdk.blueprint_cdk_stack import BlueprintCdkStack
from blueprint_cdk.my_ecs_cluster_stack import MyEcsClusterStack


app = cdk.App()


# My Stack CloudFormation
MyEcsClusterStack(app, "MyEcsClusterStack",
                    env=cdk.Environment(account='<accountn_aws_id>', region='us-east-1'),
                    )

app.synth()
