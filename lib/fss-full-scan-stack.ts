import { Construct } from 'constructs';
import { CfnCondition, CfnOutput, CfnParameter, Duration, Fn, RemovalPolicy, Stack, StackProps } from 'aws-cdk-lib';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as stepfunctions from 'aws-cdk-lib/aws-stepfunctions';
import * as tasks from 'aws-cdk-lib/aws-stepfunctions-tasks';

export class FssFullScanStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const bucketNameInput = new CfnParameter(this, 'BucketName', {
      type: 'String',
      description: 'Name of a bucket that you want to full scan. Make sure you have FSS Storage Stack deployed around it already.'
    })

    const queueArnInput = new CfnParameter(this, 'ScannerQueueArn', {
      type: 'String',
      description: 'ARN of the ScannerQueue queue. Something like arn:aws:sqs:us-east-1:123456789012:All-in-one-TM-FileStorageSecurity-ScannerStack-IT1V5O-ScannerQueue-1IOQHTGGGZYFL'
    });

    const sqsUrlInput = new CfnParameter(this, 'ScannerQueueUrl', {
      type: 'String',
      description: 'URL of the ScannerQueue queue. Something like https://sqs.us-east-1.amazonaws.com/123456789012/All-in-one-TM-FileStorageSecurity-ScannerStack-IT1V5O-ScannerQueue-1IOQHTGGGZYFL'
    });

    const topicArnInput = new CfnParameter(this, 'ScanResultTopicArn', {
      type: 'String',
      description: 'ARN of ScanResultTopic topic. Something like arn:aws:sns:us-east-1:123456789012:All-in-one-TM-FileStorageSecurity-StorageStack-1E00QCLBZW7M4-ScanResultTopic-1W7RZ7PBZZUJO'
    });

    const schedule = new CfnParameter(this, 'Schedule', {
      type: 'String',
      description: 'Set a schedule for full scan. If empty, there will not be a scheduled scan. Defaults to empty. More info at: https://docs.aws.amazon.com/lambda/latest/dg/services-cloudwatchevents-expressions.html',
      default: '',
    });

    const setSchedule = new CfnCondition(this, 'SetSchedule', {
      expression: Fn.conditionNot(Fn.conditionEquals('', schedule.valueAsString))
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
      resources: [`${bucket.bucketArn}/*`],
      actions: ['s3:GetObject', 's3:PutObjectTagging'],
    }));

    executionRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      resources: [queueArn],
      actions: ['sqs:SendMessage'],
    }));

    const scanStarterFunction = new lambda.Function(this, 'ScanStarter', {
      runtime: lambda.Runtime.PYTHON_3_8,
      handler: 'index.handler',
      role: executionRole,
      timeout: Duration.minutes(10),
      environment: {
        'SNSArn': topicArn,
        'SQSUrl': sqsUrl,
        'BucketToScanName': bucket.bucketName,
      },
      code: lambda.InlineCode.fromInline(`
# Copyright (C) 2021 Trend Micro Inc. All rights reserved.

import json
import os
import logging
import boto3
import botocore
from botocore.config import Config
from botocore.exceptions import ClientError
import urllib.parse
import uuid
import datetime

sqs_url = os.environ['SQSUrl']
print('scanner queue URL: ' + sqs_url)
sqs_region = sqs_url.split('.')[1]
print('scanner queue region: ' + sqs_region)
sqs_endpoint_url = 'https://sqs.{0}.amazonaws.com'.format(sqs_region)
print('scanner queue endpoint URL: ' + sqs_endpoint_url)
report_object_key = 'True' == os.environ.get('REPORT_OBJECT_KEY', 'False')
print(f'report object key: {report_object_key}')

region = boto3.session.Session().region_name
s3_client_path = boto3.client('s3', region, config=Config(s3={'addressing_style': 'path'}, signature_version='s3v4'))
s3_client_virtual = boto3.client('s3', region, config=Config(s3={'addressing_style': 'virtual'}, signature_version='s3v4'))

try:
    with open('version.json') as version_file:
        version = json.load(version_file)
        print(f'version: {version}')
except Exception as ex:
    print('failed to get version: ' + str(ex))

def create_presigned_url(bucket_name, object_name, expiration):
    """Generate a presigned URL to share an S3 object

    :param bucket_name: string
    :param object_name: string
    :param expiration: Time in seconds for the presigned URL to remain valid
    :return: Presigned URL as string. If error, returns None.
    """

    # Generate a presigned URL for the S3 object
    try:
        s3_client = s3_client_path if '.' in bucket_name else s3_client_virtual
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


def push_to_sqs(bucket_name, object_name, amz_request_id, presigned_url, event_time):
    object = {
        'S3': {
            'bucket': bucket_name,
            'object': object_name,
            'amzRequestID': amz_request_id,
        },
        'ScanID': str(uuid.uuid4()),
        'SNS' : os.environ['SNSArn'],
        'URL': presigned_url,
        'ModTime': event_time,
        'ReportObjectKey': report_object_key
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

def is_folder(key):
    return key.endswith('/')

def handle_step_functions_event(bucket, key):
    key = urllib.parse.unquote_plus(key)
    amz_request_id = "f"
    event_time = datetime.datetime.utcnow().isoformat() # ISO-8601 format, 1970-01-01T00:00:00.000Z, when Amazon S3 finished processing the request

    if is_folder(key):
        print('Skip scanning for folder.')
        return

    presigned_url = create_presigned_url(
        bucket,
        key,
        expiration = 60 * 60 # in seconds
    )
    print(f'AMZ request ID: {amz_request_id}, event time: {event_time}, URL:', presigned_url.split('?')[0])
    sqs_response = push_to_sqs(bucket, key, amz_request_id, presigned_url, event_time)
    print(sqs_response)

def lambda_handler(event, context):
    print('event:' + json.dumps(event))
    print('boto3 version:' + boto3.__version__)
    print('botocore version:' + botocore.__version__)
    bucket = os.environ['BucketToScanName']
    key = event['Key']
    handle_step_functions_event(bucket, key)
      `)
    });

    const scanOnSchedule = new events.Rule(this, 'ScanOnSchedule', {
      schedule: events.Schedule.expression(schedule.valueAsString),
      targets: [new targets.LambdaFunction(scanStarterFunction)],
    });
    const cfnScanOnSchedule = scanOnSchedule.node.defaultChild as events.CfnRule;
    cfnScanOnSchedule.cfnOptions.condition = setSchedule;
    const cfnFullScanner = scanStarterFunction.permissionsNode.defaultChild as lambda.CfnPermission;
    cfnFullScanner.cfnOptions.condition = setSchedule;

    const definition = new tasks.CallAwsService(this, 'ListObjectsV2', {
      action: 'ListObjectsV2',
      service: 'S3',
      iamResources: [bucket.bucketArn],
      iamAction: 's3:ListBucket',
      parameters: {
        'Bucket': bucket.bucketName
      },

    }).next(new stepfunctions.Map(this, 'Map', {
      maxConcurrency: 40,
      itemsPath: '$.Contents',

    }).iterator(new tasks.LambdaInvoke(this, 'CallLambdaFunction', {
      lambdaFunction: scanStarterFunction,
      outputPath: "$.Payload",
    })))

    const fullScanStateMachine  = new stepfunctions.StateMachine(this, 'SimpleStateMachine', {
      definition: definition
    });

    new CfnOutput(this, 'FullScanStepFunctionsPage', {
      value: `https://${this.region}.console.aws.amazon.com/states/home?region=${this.region}#/statemachines/view/${fullScanStateMachine.stateMachineArn}`
    });

  };
}