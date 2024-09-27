# AWS::KafkaConnect::CustomPlugin CustomPluginFileDescription

Details about the custom plugin file.

## Syntax

To declare this entity in your AWS CloudFormation template, use the following syntax:

### JSON

<pre>
{
    "<a href="#filemd5" title="FileMd5">FileMd5</a>" : <i>String</i>,
    "<a href="#filesize" title="FileSize">FileSize</a>" : <i>Integer</i>
}
</pre>

### YAML

<pre>
<a href="#filemd5" title="FileMd5">FileMd5</a>: <i>String</i>
<a href="#filesize" title="FileSize">FileSize</a>: <i>Integer</i>
</pre>

## Properties

#### FileMd5

The hex-encoded MD5 checksum of the custom plugin file. You can use it to validate the file.

_Required_: No

_Type_: String

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### FileSize

The size in bytes of the custom plugin file. You can use it to validate the file.

_Required_: No

_Type_: Integer

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)
