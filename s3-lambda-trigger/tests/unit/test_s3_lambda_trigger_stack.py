import aws_cdk as core
import aws_cdk.assertions as assertions

from s3_lambda_trigger.s3_lambda_trigger_stack import S3LambdaTriggerStack

# example tests. To run these tests, uncomment this file along with the example
# resource in s3_lambda_trigger/s3_lambda_trigger_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = S3LambdaTriggerStack(app, "s3-lambda-trigger")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
