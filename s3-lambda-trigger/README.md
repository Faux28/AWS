
# Welcome to your CDK Python project!


1.Create a new directory
    mkdir S3-LAMBDA-TRIGGER

2.Go to the created directory
    cd S3-LAMBDA-TRIGGER

3.configure the AWS account
    aws configure

4.create and activate the virtual environment(windows)
    python -m venv .venv
    .venv/bin/activate

5.install the required dependencies
    pip install -r requirements.txt

6.create a cdk project in python
    cdk init app --language python

7.create a lambda directory
    mkdir lambda

8.copy and paste the code in appropriate files

9.bootstrap your aws account
    cdk bootstrap

9.synthesize the CloudFormation template for this code.
    cdk synth

10.deploy the stack in aws
    cdk deploy


## Useful commands

 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation
