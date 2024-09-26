import aws_cdk as core
import aws_cdk.assertions as assertions

from blueprint_cdk.blueprint_cdk_stack import BlueprintCdkStack

# example tests. To run these tests, uncomment this file along with the example
# resource in blueprint_cdk/blueprint_cdk_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = BlueprintCdkStack(app, "blueprint-cdk")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
