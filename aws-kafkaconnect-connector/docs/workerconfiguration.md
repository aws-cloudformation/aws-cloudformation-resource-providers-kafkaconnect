# AWS::KafkaConnect::Connector WorkerConfiguration

Specifies the worker configuration to use with the connector.

## Syntax

To declare this entity in your AWS CloudFormation template, use the following syntax:

### JSON

<pre>
{
    "<a href="#revision" title="Revision">Revision</a>" : <i>Integer</i>,
    "<a href="#workerconfigurationarn" title="WorkerConfigurationArn">WorkerConfigurationArn</a>" : <i>String</i>
}
</pre>

### YAML

<pre>
<a href="#revision" title="Revision">Revision</a>: <i>Integer</i>
<a href="#workerconfigurationarn" title="WorkerConfigurationArn">WorkerConfigurationArn</a>: <i>String</i>
</pre>

## Properties

#### Revision

The revision of the worker configuration to use.

_Required_: Yes

_Type_: Integer

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### WorkerConfigurationArn

The Amazon Resource Name (ARN) of the worker configuration to use.

_Required_: Yes

_Type_: String

_Pattern_: <code>arn:(aws|aws-us-gov|aws-cn):kafkaconnect:.*</code>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)
