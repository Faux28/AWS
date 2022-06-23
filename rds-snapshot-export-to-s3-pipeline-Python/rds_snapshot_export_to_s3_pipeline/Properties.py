import boto3
import re

sts = boto3.client("sts")
account_arn = sts.get_caller_identity()["Arn"]




props={"dbName":"<existing-database-name>","rdsEventId":"RDS-EVENT-0091"} #enter a existing database name