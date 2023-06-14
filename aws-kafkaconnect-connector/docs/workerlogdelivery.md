# AWS::KafkaConnect::Connector WorkerLogDelivery

Specifies where worker logs are delivered.

## Syntax

To declare this entity in your AWS CloudFormation template, use the following syntax:

### JSON

<pre>
{
    "<a href="#cloudwatchlogs" title="CloudWatchLogs">CloudWatchLogs</a>" : <i><a href="cloudwatchlogslogdelivery.md">CloudWatchLogsLogDelivery</a></i>,
    "<a href="#firehose" title="Firehose">Firehose</a>" : <i><a href="firehoselogdelivery.md">FirehoseLogDelivery</a></i>,
    "<a href="#s3" title="S3">S3</a>" : <i><a href="s3logdelivery.md">S3LogDelivery</a></i>
}
</pre>

### YAML

<pre>
<a href="#cloudwatchlogs" title="CloudWatchLogs">CloudWatchLogs</a>: <i><a href="cloudwatchlogslogdelivery.md">CloudWatchLogsLogDelivery</a></i>
<a href="#firehose" title="Firehose">Firehose</a>: <i><a href="firehoselogdelivery.md">FirehoseLogDelivery</a></i>
<a href="#s3" title="S3">S3</a>: <i><a href="s3logdelivery.md">S3LogDelivery</a></i>
</pre>

## Properties

#### CloudWatchLogs

Details about delivering logs to Amazon CloudWatch Logs.

_Required_: No

_Type_: <a href="cloudwatchlogslogdelivery.md">CloudWatchLogsLogDelivery</a>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### Firehose

Details about delivering logs to Amazon Kinesis Data Firehose.

_Required_: No

_Type_: <a href="firehoselogdelivery.md">FirehoseLogDelivery</a>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### S3

Details about delivering logs to Amazon S3.

_Required_: No

_Type_: <a href="s3logdelivery.md">S3LogDelivery</a>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

