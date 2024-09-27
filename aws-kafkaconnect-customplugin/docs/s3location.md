# AWS::KafkaConnect::CustomPlugin S3Location

The S3 bucket Amazon Resource Name (ARN), file key, and object version of the plugin file stored in Amazon S3.

## Syntax

To declare this entity in your AWS CloudFormation template, use the following syntax:

### JSON

<pre>
{
    "<a href="#bucketarn" title="BucketArn">BucketArn</a>" : <i>String</i>,
    "<a href="#filekey" title="FileKey">FileKey</a>" : <i>String</i>,
    "<a href="#objectversion" title="ObjectVersion">ObjectVersion</a>" : <i>String</i>
}
</pre>

### YAML

<pre>
<a href="#bucketarn" title="BucketArn">BucketArn</a>: <i>String</i>
<a href="#filekey" title="FileKey">FileKey</a>: <i>String</i>
<a href="#objectversion" title="ObjectVersion">ObjectVersion</a>: <i>String</i>
</pre>

## Properties

#### BucketArn

The Amazon Resource Name (ARN) of an S3 bucket.

_Required_: Yes

_Type_: String

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### FileKey

The file key for an object in an S3 bucket.

_Required_: Yes

_Type_: String

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### ObjectVersion

The version of an object in an S3 bucket.

_Required_: No

_Type_: String

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)
