
# Welcome to your CDK Python project!

## Steps to create cdk stack to export rds snapshot to s3

1.Create a new directory  

```
mkdir rds-snapshot-export-to-s3-pipeline-Python
```
    
2.Go to the created directory  

```
cd rds-snapshot-export-to-s3-pipeline-Python
```

3.configure the AWS account  

```
aws configure
```

4.create and activate the virtual environment(windows)  

```
python -m venv .venv
```  
```
.venv/bin/activate
```

5.install the required dependencies.

```
pip install -r requirements.txt
```

6.create a cdk project in python  

```
cdk init app --language python
```

7.create a lambda directory  

```
mkdir lambda
```

8.copy and paste the code in appropriate files  

9.bootstrap your aws account  

```
cdk bootstrap
```

9.synthesize the CloudFormation template for this code  

```
cdk synth
```

10.deploy the stack in aws  
 ```
 cdk deploy
 ```


## Useful commands

 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation


## To Test the code use event.json
 
 * enter the snapshot name.
 * configure the test event in aws console.
 * test the code.


## Glue Crawler 

 * Crawler should be run manually by user when required.  
 it is a paid service.
