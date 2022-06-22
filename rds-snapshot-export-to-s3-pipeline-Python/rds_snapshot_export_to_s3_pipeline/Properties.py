import boto3
import re

sts = boto3.client("sts")
account_arn = sts.get_caller_identity()["Arn"]

try:
    kms = boto3.client("kms")
    keys = kms.list_keys()['Keys']
    for keys in keys:
        desc = kms.describe_key(KeyId=keys['KeyId'])
        check = desc['KeyMetadata']['Description']
        if re.search("RDS", check):
            RDS_key_ARN = keys['KeyArn']
except:
    pass


props={"dbName":"<existing-database-name>","rdsEventId":"RDS-EVENT-0091"} #enter a existing database name