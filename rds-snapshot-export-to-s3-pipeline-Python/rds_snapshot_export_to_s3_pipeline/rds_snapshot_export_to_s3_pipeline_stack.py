from typing_extensions import Self
from aws_cdk import (
    Stack,
    aws_glue as glue,
    aws_iam as iam,
    aws_lambda as awslambda,
    aws_sns_subscriptions as sns_subscription,
    aws_kms as kms,
    aws_rds as rds,
    aws_s3 as s3,
    aws_sns as sns
)
from constructs import Construct

from Properties import props, account_arn

class RdsSnapshotExportToS3PipelineStack(Stack):

    def __init__(self, scope: Construct, construct_id: str,**kwargs) -> None:
        super().__init__(scope, construct_id,**kwargs)

        snapshotbucket = s3.Bucket(self, "Bucket",bucket_name="<bucket-name>") #enter the bucket name

        snapshotExportTaskRole = iam.Role(self, "snapshotExportTaskRole",
                            
                            assumed_by=iam.ServicePrincipal("export.rds.amazonaws.com"),
                            description="Role used by RDS to perform snapshot exports to S3")

        snapshotExportTaskRole.attach_inline_policy(iam.Policy(self,"exportpolicy",policy_name="snapshotExportTaskpolicy",statements=[
                                iam.PolicyStatement(
                                    actions=["s3:PutObject*",
                                            "s3:ListBucket",
                                            "s3:GetObject*",
                                            "s3:DeleteObject*",
                                            "s3:GetBucketLocation"],
                                    resources=[snapshotbucket.bucket_arn,snapshotbucket.bucket_arn+"/*"])
                                    ]))

        lambdarole = iam.Role(self, "lambdarole",
                            
                            description="RdsSnapshotExportToS3 Lambda execution role for the database.",
                            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"))

        lambdarole.attach_inline_policy(iam.Policy(self,"lambdapolicy",policy_name="lambdaExecutionpolicy",statements=[
                                iam.PolicyStatement(actions=["rds:StartExportTask"],resources=["*"]),
                                iam.PolicyStatement(actions=["iam:PassRole"],resources=[snapshotExportTaskRole.role_arn])
                                ]))

        lambdarole.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"))

        gluecrawlerrole = iam.Role(self, "snapshot_export_gluecrawler_role",
                            
                            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"))

        gluecrawlerrole.attach_inline_policy(iam.Policy(self,"gluepolicy",policy_name="snapshotExportGlueCrawlerpolicy",statements=[
                                iam.PolicyStatement(actions=["s3:GetObject","s3:PutObject"],resources=[snapshotbucket.bucket_arn+"/*"])
                                ]))

        gluecrawlerrole.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"))

        """""""""
        Customer managed key is a paid service
        """""""""      
        
        snapshotExportEncryptionKey=kms.Key(self,"snapshotExportEncryptionKey",alias=props['dbName']+"-snapshot-exports",
        policy=iam.PolicyDocument.from_json({
          "Version": "2012-10-17",
          "Id": "key-default-1",
          "Statement": [
            {
            "Sid": "Enable IAM User Permissions",
            "Effect": "Allow",
            "Principal": {
                "AWS": iam.AccountRootPrincipal().arn
            },
            "Action": "kms:*",
            "Resource": "*"
        }, {
            "Sid": "Allow access for Key Administrators",
            "Effect": "Allow",
            "Principal": {
                "AWS": account_arn
            },
            "Action": [
                "kms:Create*",
                "kms:Describe*",
                "kms:Enable*",
                "kms:List*",
                "kms:Put*",
                "kms:Update*",
                "kms:Revoke*",
                "kms:Disable*",
                "kms:Get*",
                "kms:Delete*",
                "kms:TagResource",
                "kms:UntagResource",
                "kms:ScheduleKeyDeletion",
                "kms:CancelKeyDeletion"
            ],
            "Resource": "*"
        },
              {"Sid": "Allow use of the key",
            "Effect": "Allow",
                    "Principal" : {
                        "AWS" : [
                            iam.AccountRootPrincipal().arn,
                            lambdarole.role_arn,
                            gluecrawlerrole.role_arn,
                            account_arn
                        ]
                    },
                    "Action":["kms:Encrypt",
                        "kms:Decrypt",
                        "kms:ReEncrypt*",
                        "kms:GenerateDataKey*",
                        "kms:DescribeKey"],
                    "Resource": "*"
                },
                {"Sid": "Allow attachment of persistent resources",
            "Effect": "Allow",
                    "Principal":{ 
                        "AWS" :[lambdarole.role_arn,
                            iam.AccountRootPrincipal().arn,
                            account_arn]}, 
                    "Action": [
                        "kms:CreateGrant",
                        "kms:ListGrants",
                        "kms:RevokeGrant"
                        ],
                         "Resource": "*"
                }
          ]
      }
  
   ),
            )
            
        snapshotEventTopic= sns.Topic(self,"SnapshotEventTopic",topic_name="SnapshotEventTopic")

        rds.CfnEventSubscription(self,"RdsSnapshotEventNotification",sns_topic_arn=snapshotEventTopic.topic_arn,enabled=True,event_categories=["creation"],source_type="db-snapshot")

        lambdafunction = awslambda.Function(self, "Lambda",
                                    function_name="ffi-s3-lambda-rds-pipeline-function",
                                    runtime=awslambda.Runtime.PYTHON_3_9,
                                    handler="lambda_listener.main",
                                    code=awslambda.Code.from_asset("./lambda"),
                                    environment= {
                                    "RDS_EVENT_ID": props['rdsEventId'],
                                    "DB_NAME": props['dbName'],
                                    "LOG_LEVEL": "INFO",
                                    "SNAPSHOT_BUCKET_NAME": snapshotbucket.bucket_name,
                                    "SNAPSHOT_TASK_ROLE": snapshotExportTaskRole.role_arn,
                                    "SNAPSHOT_TASK_KEY": snapshotExportEncryptionKey.key_arn,
                                    "DB_SNAPSHOT_TYPE": "snapshot",
                                     },
                                    role=lambdarole)


        snapshotEventTopic.add_subscription(sns_subscription.LambdaSubscription(lambdafunction))


        glue.CfnCrawler(self,"SnapshotExportCrawler",
            name=props['dbName']+"-rds-snapshot-crawler",
            role=gluecrawlerrole.role_arn,
            
            targets=glue.CfnCrawler.TargetsProperty(s3_targets=[glue.CfnCrawler.S3TargetProperty(
            path=snapshotbucket.bucket_name,exclusions=["*.json"])]),
            database_name=props['dbName']
            )
    