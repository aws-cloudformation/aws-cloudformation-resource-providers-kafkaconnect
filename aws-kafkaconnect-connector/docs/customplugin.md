# AWS::KafkaConnect::Connector CustomPlugin

Details about a custom plugin.

## Syntax

To declare this entity in your AWS CloudFormation template, use the following syntax:

### JSON

<pre>
{
    "<a href="#custompluginarn" title="CustomPluginArn">CustomPluginArn</a>" : <i>String</i>,
    "<a href="#revision" title="Revision">Revision</a>" : <i>Integer</i>
}
</pre>

### YAML

<pre>
<a href="#custompluginarn" title="CustomPluginArn">CustomPluginArn</a>: <i>String</i>
<a href="#revision" title="Revision">Revision</a>: <i>Integer</i>
</pre>

## Properties

#### CustomPluginArn

The Amazon Resource Name (ARN) of the custom plugin to use.

_Required_: Yes

_Type_: String

_Pattern_: <code>arn:(aws|aws-us-gov|aws-cn):kafkaconnect:.*</code>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### Revision

The revision of the custom plugin to use.

_Required_: Yes

_Type_: Integer

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

