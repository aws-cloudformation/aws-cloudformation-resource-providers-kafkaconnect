# AWS::KafkaConnect::Connector Vpc

Information about a VPC used with the connector.

## Syntax

To declare this entity in your AWS CloudFormation template, use the following syntax:

### JSON

<pre>
{
    "<a href="#securitygroups" title="SecurityGroups">SecurityGroups</a>" : <i>[ String, ... ]</i>,
    "<a href="#subnets" title="Subnets">Subnets</a>" : <i>[ String, ... ]</i>
}
</pre>

### YAML

<pre>
<a href="#securitygroups" title="SecurityGroups">SecurityGroups</a>: <i>
      - String</i>
<a href="#subnets" title="Subnets">Subnets</a>: <i>
      - String</i>
</pre>

## Properties

#### SecurityGroups

The AWS security groups to associate with the elastic network interfaces in order to specify what the connector has access to.

_Required_: Yes

_Type_: List of String

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### Subnets

The list of subnets to connect to in the virtual private cloud (VPC). AWS creates elastic network interfaces inside these subnets.

_Required_: Yes

_Type_: List of String

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

