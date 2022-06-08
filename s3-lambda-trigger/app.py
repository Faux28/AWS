#!/usr/bin/env python3
import os

import aws_cdk as cdk

from s3_lambda_trigger.s3_lambda_trigger_stack import S3LambdaTriggerStack


app = cdk.App()
S3LambdaTriggerStack(app, "S3LambdaTriggerStack")

app.synth()
