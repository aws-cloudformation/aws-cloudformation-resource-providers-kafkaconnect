# AWS::KafkaConnect::Connector KafkaClusterClientAuthentication

Details of the client authentication used by the Kafka cluster.

## Syntax

To declare this entity in your AWS CloudFormation template, use the following syntax:

### JSON

<pre>
{
    "<a href="#authenticationtype" title="AuthenticationType">AuthenticationType</a>" : <i>String</i>
}
</pre>

### YAML

<pre>
<a href="#authenticationtype" title="AuthenticationType">AuthenticationType</a>: <i>String</i>
</pre>

## Properties

#### AuthenticationType

The type of client authentication used to connect to the Kafka cluster. Value NONE means that no client authentication is used.

_Required_: Yes

_Type_: String

_Allowed Values_: <code>NONE</code> | <code>IAM</code>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)
