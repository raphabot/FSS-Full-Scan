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

    // Scan Starter
    const scanStarterExecutionRole = new iam.Role(this, 'ScanStarterExecutionRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole')]
    });

    scanStarterExecutionRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      resources: [`${bucket.bucketArn}/*`],
      actions: ['s3:GetObject', 's3:PutObjectTagging'],
    }));

    scanStarterExecutionRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      resources: [queueArn],
      actions: ['sqs:SendMessage'],
    }));

    const scanStarterFunction = new lambda.Function(this, 'ScanStarterFunction', {
      runtime: lambda.Runtime.PYTHON_3_9,
      handler: 'index.lambda_handler',
      role: scanStarterExecutionRole,
      timeout: Duration.minutes(1),
      memorySize: 128,
      environment: {
        'SNSArn': topicArn,
        'SQSUrl': sqsUrl,
        'BucketToScanName': bucket.bucketName,
      },
      code: lambda.Code.fromAsset('./lambda/scanner')
    });

    // Paginator
    const paginatorExecutionRole = new iam.Role(this, 'PaginatorExecutionRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole')]
    });

    paginatorExecutionRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      resources: [`${bucket.bucketArn}`],
      actions: ['s3:ListBucket'],
    }));

    paginatorExecutionRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      resources: [`${scanStarterFunction.functionArn}`],
      actions: ['lambda:InvokeFunction'],
    }));

    const paginatorFunction = new lambda.Function(this, 'PaginatorFunction', {
      runtime: lambda.Runtime.NODEJS_14_X,
      architecture: lambda.Architecture.ARM_64,
      handler: 'index.lambda_handler',
      role: paginatorExecutionRole,
      timeout: Duration.minutes(15),
      memorySize: 256,
      environment: {
        'BUCKET_NAME': bucket.bucketName,
        'SCAN_FUNCTION_NAME': scanStarterFunction.functionArn,
      },
      code: lambda.Code.fromAsset('./lambda/paginator')
    });

    // const scanOnSchedule = new events.Rule(this, 'ScanOnSchedule', {
    //   schedule: events.Schedule.expression(schedule.valueAsString),
    //   targets: [new targets.LambdaFunction(paginatorFunction)],
    // });
    // const cfnScanOnSchedule = scanOnSchedule.node.defaultChild as events.CfnRule;
    // cfnScanOnSchedule.cfnOptions.condition = setSchedule;
    // const cfnFullScanner = paginatorFunction.permissionsNode.defaultChild as lambda.CfnPermission;
    // cfnFullScanner.cfnOptions.condition = setSchedule;

    new CfnOutput(this, 'FullScanFunctionPage', {
      value: `https://${this.region}.console.aws.amazon.com/lambda/home?region=${this.region}#/functions/${paginatorFunction.functionName}?tab=code`
    });

  };
}