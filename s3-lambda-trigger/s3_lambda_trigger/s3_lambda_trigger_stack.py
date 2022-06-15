from aws_cdk import (
    aws_lambda as _lambda,
    aws_s3 as _s3,
    aws_s3_notifications,
    aws_iam as iam,
    Stack
)
from constructs import Construct


class S3LambdaTriggerStack(Stack):

    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        iam_role = iam.Role(self, "Role",
                            role_name="ffi-s3-lambda-trigger-role",
                            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"))

        iam_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"))

        iam_role.add_managed_policy(iam.ManagedPolicy.from_managed_policy_name(self,"s3copytrigger","AmazonS3ReadandWriteAccess"))


        function = _lambda.Function(self, "Lambda",
                                    function_name="ffi-s3-lambda-trigger-function",
                                    runtime=_lambda.Runtime.PYTHON_3_9,
                                    handler="lambda_listener.main",
                                    code=_lambda.Code.from_asset("./lambda"),
                                    role=iam_role)

        s3 = _s3.Bucket(self, "Bucket",bucket_name="ffi-s3-lambda-trigger-bucket")

        lambda_trigger = aws_s3_notifications.LambdaDestination(function)

        lambda_trigger.bind(self,s3)

        s3.add_event_notification(_s3.EventType.OBJECT_CREATED, lambda_trigger)

        s3.add_event_notification(_s3.EventType.OBJECT_REMOVED, lambda_trigger)

       

