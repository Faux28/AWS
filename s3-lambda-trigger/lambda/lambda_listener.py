import boto3

import re


s3 = boto3.client('s3')

def copy_func(event, context):
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    target_bucket = 'ffi-test'
    copy_source = {'Bucket': source_bucket, 'Key': object_key}
    print ("Source bucket : ", source_bucket)
    print ("Target bucket : ", target_bucket)
    print ("Log Stream name: ", context.log_stream_name)
    print ("Mem. limits(MB): ", context.memory_limit_in_mb)
    try:
        s3.copy_object(Bucket=target_bucket, Key=object_key, CopySource=copy_source)
        print("object created successfully")
    except Exception as err:
        print ("Error -"+str(err))


def main(event, context):

    print(event)

    for e in event['Records']:

        if bool(re.search("ObjectCreated",e['eventName'] )):
            copy_func(event,context)

        elif bool(re.search("ObjectRemoved",e['eventName'] )):
            print ("object deleted successfully")
        
    return {
        'statusCode': 200,
        'body': event
    }