# AWS::KafkaConnect::Connector ApacheKafkaCluster

Details of how to connect to an Apache Kafka cluster.

## Syntax

To declare this entity in your AWS CloudFormation template, use the following syntax:

### JSON

<pre>
{
    "<a href="#bootstrapservers" title="BootstrapServers">BootstrapServers</a>" : <i>String</i>,
    "<a href="#vpc" title="Vpc">Vpc</a>" : <i><a href="vpc.md">Vpc</a></i>
}
</pre>

### YAML

<pre>
<a href="#bootstrapservers" title="BootstrapServers">BootstrapServers</a>: <i>String</i>
<a href="#vpc" title="Vpc">Vpc</a>: <i><a href="vpc.md">Vpc</a></i>
</pre>

## Properties

#### BootstrapServers

The bootstrap servers string of the Apache Kafka cluster.

_Required_: Yes

_Type_: String

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### Vpc

Information about a VPC used with the connector.

_Required_: Yes

_Type_: <a href="vpc.md">Vpc</a>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)
