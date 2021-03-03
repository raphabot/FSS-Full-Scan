import * as cdk from '@aws-cdk/core';
import * as lambda from '@aws-cdk/aws-lambda';
import * as s3 from '@aws-cdk/aws-s3';
import * as iam from '@aws-cdk/aws-iam';

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

    const func = new lambda.Function(this, 'BucketFullScan', {
      runtime: lambda.Runtime.PYTHON_3_8,
      handler: 'index.lambda_handler',
      role: executionRole,
      timeout: cdk.Duration.seconds(60),
      environment: {
        'SNSArn': topicArn,
        'SQSUrl': sqsUrl,
        'BucketName': bucket.bucketName
      },
      code: lambda.InlineCode.fromInline(`
import json
import os
import logging
import boto3
import botocore
from botocore.config import Config
from botocore.exceptions import ClientError
import urllib.parse
import uuid

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


def push_to_sqs(bucket_name, object_name, presigned_url, sqs_name, event_time):
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
        print(sqs_name)
        region = sqs_name.split('.')[1]
        print(region)
        session = boto3.session.Session(region_name=region)
        sqs = session.resource(service_name='sqs', endpoint_url="https://sqs." + region + ".amazonaws.com")
        queue = sqs.Queue(url=sqs_name)
        response = queue.send_message(MessageBody=json.dumps(object))
        return response
    except ClientError as e:
        print('failed to push SQS message: ' + str(e))
        return None


def lambda_handler(event, context):
    
    bucketName = os.environ['BucketName']
    print(bucketName)
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucketName)
    sqs_name = os.environ['SQSUrl']
    for obj in bucket.objects.all():
        from datetime import datetime
        event_time = datetime.now().isoformat()
        key = obj.key
        print(key)
        presigned_url = create_presigned_url(
            bucketName,
            key,
            expiration = 60 * 60 # in seconds
        )
        print(f'event time: {event_time}, URL:', presigned_url.split('?')[0])
        sqs_response = push_to_sqs(bucket, key, presigned_url, sqs_name, event_time)
        print(sqs_response)
      `)
    });

  };
}
