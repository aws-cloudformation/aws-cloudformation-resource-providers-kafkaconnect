# AWS::KafkaConnect::CustomPlugin CustomPluginLocation

Information about the location of a custom plugin.

## Syntax

To declare this entity in your AWS CloudFormation template, use the following syntax:

### JSON

<pre>
{
    "<a href="#s3location" title="S3Location">S3Location</a>" : <i><a href="s3location.md">S3Location</a></i>
}
</pre>

### YAML

<pre>
<a href="#s3location" title="S3Location">S3Location</a>: <i><a href="s3location.md">S3Location</a></i>
</pre>

## Properties

#### S3Location

The S3 bucket Amazon Resource Name (ARN), file key, and object version of the plugin file stored in Amazon S3.

_Required_: Yes

_Type_: <a href="s3location.md">S3Location</a>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)
