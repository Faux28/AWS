from typing_extensions import Self
from aws_cdk import (
    Stack,
    aws_sqs as sqs,
    aws_kms as kms,
    aws_glue as glue,
    aws_iam as iam,
    aws_lambda as awslambda,
    aws_lambda_event_sources as lambda_event_sources,
    aws_rds as rds,
    aws_s3 as s3,
    aws_sns as sns
)
import path as path
from constructs import Construct
from enum import Enum

# def RdsEventId():
#     DB_AUTOMATED_SNAPSHOT_CREATED = "RDS-EVENT-0091"
#     return DB_AUTOMATED_SNAPSHOT_CREATED


props={"dbName":"","rdsEventId":"RDS-EVENT-0091"}
class RdsSnapshotExportToS3PipelineStack(Stack):

    def __init__(self, scope: Construct, construct_id: str,**kwargs) -> None:
        super().__init__(scope, construct_id,**kwargs)

        snapshotbucket = s3.Bucket(self, "Bucket",bucket_name="ffi-s3-lambda-rds-pipeline-bucket")

        snapshotExportTaskRole = iam.Role(self, "snapshotExportTaskRole",
                            role_name="snapshotExportTaskRole",
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

        lambdaExecutionRole = iam.Role(self, "lambdaExecutionRole",
                            role_name="RdsSnapshotExporterLambdaExecutionRole ",
                            description="RdsSnapshotExportToS3 Lambda execution role for the database.",
                            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"))

        lambdaExecutionRole.attach_inline_policy(iam.Policy(self,"lambdapolicy",policy_name="lambdaExecutionpolicy",statements=[
                                iam.PolicyStatement(actions=["rds:StartExportTask"],resources=["*"]),
                                iam.PolicyStatement(actions=["iam:PassRole"],resources=[snapshotExportTaskRole.role_arn])
                                ]))

        lambdaExecutionRole.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"))

        snapshotExportGlueCrawlerRole = iam.Role(self, "snapshotExportGlueCrawlerRole",
                            role_name="snapshotExportGlueCrawlerRole ",
                            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"))

        snapshotExportGlueCrawlerRole.attach_inline_policy(iam.Policy(self,"gluepolicy",policy_name="snapshotExportGlueCrawlerpolicy",statements=[
                                iam.PolicyStatement(actions=["s3:GetObject","s3:PutObject"],resources=[snapshotbucket.bucket_arn+"/*"])
                                ]))

        snapshotExportGlueCrawlerRole.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"))

        snapshotEventTopic= sns.Topic(self,"SnapshotEventTopic",topic_name="SnapshotEventTopic")

        RdsSnapshotEventNotification = rds.CfnEventSubscription(self,"RdsSnapshotEventNotification",sns_topic_arn=snapshotEventTopic.topic_arn,enabled=True,event_categories=["creation"],source_type="db-snapshot")

        function = awslambda.Function(self, "Lambda",
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
        # "SNAPSHOT_TASK_KEY": snapshotExportEncryptionKey.keyArn,
        "DB_SNAPSHOT_TYPE": "snapshot",
         },
        role=lambdaExecutionRole)

        event_source=lambda_event_sources.SqsEventSource(snapshotEventTopic)

        function.add_event_source(event_source)

        event_source_id = event_source.event_source_mapping_id


        SnapshotExportCrawler=glue.CfnCrawler(self,"SnapshotExportCrawler",name=props['dbName']+"-rds-snapshot-crawler",
        role=snapshotExportGlueCrawlerRole.role_arn,targets={"s3Targets":{"path":snapshotbucket.bucket_name}},
            database_name=props['dbName'],schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
        delete_behavior="deleteBehavior"))

     