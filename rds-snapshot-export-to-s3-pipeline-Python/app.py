#!/usr/bin/env python3
import os

import aws_cdk as cdk

from rds_snapshot_export_to_s3_pipeline.rds_snapshot_export_to_s3_pipeline_stack import RdsSnapshotExportToS3PipelineStack


app = cdk.App()

RdsSnapshotExportToS3PipelineStack(app, "RdsSnapshotExportToS3PipelineStack",)

app.synth()
