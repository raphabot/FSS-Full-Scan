import boto3
import json

def get_matching_s3_objects(bucket):
  s3 = boto3.client("s3")
  paginator = s3.get_paginator("list_objects_v2")
    
  kwargs = {'Bucket': bucket}
  
  bucket_object_list = []
  for page in paginator.paginate(**kwargs):
    if "Contents" in page:
      for key in page[ "Contents" ]:
        keyString = json.dumps(key[ "Key" ])
        bucket_object_list.append(keyString)
  return bucket_object_list

def lambda_handler(event, context):  
  keys = get_matching_s3_objects('s3-assessment-test')
  result = {
      'Keys': keys
  }
  return result