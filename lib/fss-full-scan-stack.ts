import * as cdk from '@aws-cdk/core';
import * as lambda from '@aws-cdk/aws-lambda';
import * as s3 from '@aws-cdk/aws-s3';
import * as iam from '@aws-cdk/aws-iam';
import * as events from '@aws-cdk/aws-events';
import * as targets from '@aws-cdk/aws-events-targets';

export class FssFullScanStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const bucketNameInput = new cdk.CfnParameter(this, 'BucketName', {
      type: 'String',
      description: 'Name of a bucket that you want to full scan. Make sure you have FSS Storage Stack deployed around it already.'
    })

    const queueArnInput = new cdk.CfnParameter(this, 'ScannerQueueArn', {
      type: 'String',
      description: 'ARN of the ScannerQueue queue. Something like arn:aws:sqs:us-east-1:123456789012:All-in-one-TM-FileStorageSecurity-ScannerStack-IT1V5O-ScannerQueue-1IOQHTGGGZYFL'
    });

    const sqsUrlInput = new cdk.CfnParameter(this, 'ScannerQueueUrl', {
      type: 'String',
      description: 'URL of the ScannerQueue queue. Something like https://sqs.us-east-1.amazonaws.com/123456789012/All-in-one-TM-FileStorageSecurity-ScannerStack-IT1V5O-ScannerQueue-1IOQHTGGGZYFL'
    });

    const topicArnInput = new cdk.CfnParameter(this, 'ScanResultTopicArn', {
      type: 'String',
      description: 'ARN of ScanResultTopic topic. Something like arn:aws:sns:us-east-1:123456789012:All-in-one-TM-FileStorageSecurity-StorageStack-1E00QCLBZW7M4-ScanResultTopic-1W7RZ7PBZZUJO'
    });

    const schedule = new cdk.CfnParameter(this, 'Schedule', {
      type: 'String',
      description: 'Set a schedule for full scan. If empty, there will not be a scheduled scan. Defaults to empty. More info at: https://docs.aws.amazon.com/lambda/latest/dg/services-cloudwatchevents-expressions.html',
      default: '',
    });

    const setSchedule = new cdk.CfnCondition(this, 'SetSchedule', {
      expression: cdk.Fn.conditionNot(cdk.Fn.conditionEquals('', schedule.valueAsString))
    })

    const bucket = s3.Bucket.fromBucketName(this, 'Bucket', bucketNameInput.valueAsString);
    const queueArn = queueArnInput.valueAsString;
    const sqsUrl = sqsUrlInput.valueAsString;
    const topicArn = topicArnInput.valueAsString;

    const executionRole = new iam.Role(this, 'ExecutionRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole')]
    });

    executionRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      resources: [bucket.bucketArn],
      actions: ['s3:ListBucket', 's3:ListObjectsV2'],
    }));

    executionRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      resources: [`${bucket.bucketArn}/*`],
      actions: ['s3:GetObject', 's3:PutObjectTagging'],
    }));

    executionRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      resources: [queueArn],
      actions: ['sqs:SendMessage'],
    }));

    const fullScanner = new lambda.Function(this, 'BucketFullScan', {
      runtime: lambda.Runtime.PYTHON_3_8,
      handler: 'index.handler',
      role: executionRole,
      timeout: cdk.Duration.minutes(10),
      environment: {
        'SNSArn': topicArn,
        'SQSUrl': sqsUrl,
        'BucketToScanName': bucket.bucketName
      },
      code: lambda.InlineCode.fromInline(`
import json
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import uuid
import os
import logging
logger = logging.getLogger()

bucket=os.environ['BucketToScanName']
sqs_url = os.environ['SQSUrl']
sqs_region = sqs_url.split('.')[1]
sqs_endpoint_url = 'https://sqs.{0}.amazonaws.com'.format(sqs_region)
logger.info('## ENVIRONMENT VARIABLES')
logger.info('Bucket to be fully scanned: ' + bucket)
logger.info('Scanner queue URL: ' + sqs_url)
logger.info('Scanner queue region: ' + sqs_region)
logger.info('Scanner queue endpoint URL: ' + sqs_endpoint_url)
logger.info('SNS Arn: ' + os.environ['SNSArn'])

  
def get_matching_s3_objects(bucket, prefix="", suffix=""):
  s3 = boto3.client("s3")
  paginator = s3.get_paginator("list_objects_v2")
    
  kwargs = {'Bucket': bucket}
    
  # We can pass the prefix directly to the S3 API. If the user has passed
  # a tuple or list of prefixes, we go through them one by one.
  if isinstance(prefix, str):
    prefixes = (prefix,)
  else:
    prefixes = prefix
    
  for key_prefix in prefixes:
    kwargs["Prefix"] = key_prefix
    
  for page in paginator.paginate(**kwargs):
    try:
      contents = page["Contents"]
    except KeyError:
      break
    
    for obj in contents:
      key = obj["Key"]
      if key.endswith(suffix):
        yield obj
  
  
  
def create_presigned_url(bucket_name, object_name, expiration):
  """Generate a presigned URL to share an S3 object
    
  :param bucket_name: string
  :param object_name: string
  :param expiration: Time in seconds for the presigned URL to remain valid
  :return: Presigned URL as string. If error, returns None.
  """
    
  # Generate a presigned URL for the S3 object
  s3_client = boto3.client('s3', config=Config(s3={'addressing_style': 'virtual'}, signature_version='s3v4'))
  try:
    response = s3_client.generate_presigned_url(
      'get_object',
      Params={
      'Bucket': bucket_name,
      'Key': object_name
    },
    ExpiresIn=expiration
    )
  except ClientError as e:
    print('failed to generate pre-signed URL: ' + str(e))
    return None
    
  # The response contains the presigned URL which is sensitive data
  return response
  

def push_to_sqs(bucket_name, object_name, presigned_url, event_time):
  object = {
    'S3': {
    'bucket': "{0}".format(bucket_name),
    'object': "{0}".format(object_name)
  },
    'ScanID': str(uuid.uuid4()),
    'SNS' : os.environ['SNSArn'],
    'URL': "{0}".format(presigned_url),
    'ModTime': event_time
  }
  try:
    session = boto3.session.Session(region_name=sqs_region)
    sqs = session.resource(service_name='sqs', endpoint_url=sqs_endpoint_url)
    queue = sqs.Queue(url=sqs_url)
    response = queue.send_message(MessageBody=json.dumps(object))
    return response
  except ClientError as e:
    print('failed to push SQS message: ' + str(e))
    return None
  
def handler(event, context):  
  for object in get_matching_s3_objects(bucket=os.environ['BucketToScanName']):
    if object['Size'] == 0:
      continue
    presigned = create_presigned_url(bucket_name=bucket, object_name=object["Key"], expiration=3600)
    sqs_response = push_to_sqs(bucket, object["Key"], presigned, object['LastModified'].isoformat())
    print(sqs_response)
  return {
    'statusCode': 200,
    'body': json.dumps('Manual Scan was successfully triggered.')
  }
      `)
    });

    const scanOnSchedule = new events.Rule(this, 'ScanOnSchedule', {
      schedule: events.Schedule.expression(schedule.valueAsString),
      targets: [new targets.LambdaFunction(fullScanner)],
    });
    const cfnScanOnSchedule = scanOnSchedule.node.defaultChild as events.CfnRule;
    cfnScanOnSchedule.cfnOptions.condition = setSchedule;
    const cfnFullScanner = fullScanner.permissionsNode.defaultChild as lambda.CfnPermission;
    cfnFullScanner.cfnOptions.condition = setSchedule;

    new cdk.CfnOutput(this, 'full-scanner-lambda-page', {
      value: `https://${this.region}.console.aws.amazon.com/lambda/home?region=${this.region}#/functions/${fullScanner.functionName}?tab=code`
    });

  };
}