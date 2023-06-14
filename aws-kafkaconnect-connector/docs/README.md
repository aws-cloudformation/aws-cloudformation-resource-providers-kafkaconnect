# AWS::KafkaConnect::Connector

Resource Type definition for AWS::KafkaConnect::Connector

## Syntax

To declare this entity in your AWS CloudFormation template, use the following syntax:

### JSON

<pre>
{
    "Type" : "AWS::KafkaConnect::Connector",
    "Properties" : {
        "<a href="#capacity" title="Capacity">Capacity</a>" : <i><a href="capacity.md">Capacity</a></i>,
        "<a href="#connectorconfiguration" title="ConnectorConfiguration">ConnectorConfiguration</a>" : <i><a href="connectorconfiguration.md">ConnectorConfiguration</a></i>,
        "<a href="#connectordescription" title="ConnectorDescription">ConnectorDescription</a>" : <i>String</i>,
        "<a href="#connectorname" title="ConnectorName">ConnectorName</a>" : <i>String</i>,
        "<a href="#kafkacluster" title="KafkaCluster">KafkaCluster</a>" : <i><a href="kafkacluster.md">KafkaCluster</a></i>,
        "<a href="#kafkaclusterclientauthentication" title="KafkaClusterClientAuthentication">KafkaClusterClientAuthentication</a>" : <i><a href="kafkaclusterclientauthentication.md">KafkaClusterClientAuthentication</a></i>,
        "<a href="#kafkaclusterencryptionintransit" title="KafkaClusterEncryptionInTransit">KafkaClusterEncryptionInTransit</a>" : <i><a href="kafkaclusterencryptionintransit.md">KafkaClusterEncryptionInTransit</a></i>,
        "<a href="#kafkaconnectversion" title="KafkaConnectVersion">KafkaConnectVersion</a>" : <i>String</i>,
        "<a href="#logdelivery" title="LogDelivery">LogDelivery</a>" : <i><a href="logdelivery.md">LogDelivery</a></i>,
        "<a href="#plugins" title="Plugins">Plugins</a>" : <i>[ <a href="plugin.md">Plugin</a>, ... ]</i>,
        "<a href="#serviceexecutionrolearn" title="ServiceExecutionRoleArn">ServiceExecutionRoleArn</a>" : <i>String</i>,
        "<a href="#workerconfiguration" title="WorkerConfiguration">WorkerConfiguration</a>" : <i><a href="workerconfiguration.md">WorkerConfiguration</a></i>
    }
}
</pre>

### YAML

<pre>
Type: AWS::KafkaConnect::Connector
Properties:
    <a href="#capacity" title="Capacity">Capacity</a>: <i><a href="capacity.md">Capacity</a></i>
    <a href="#connectorconfiguration" title="ConnectorConfiguration">ConnectorConfiguration</a>: <i><a href="connectorconfiguration.md">ConnectorConfiguration</a></i>
    <a href="#connectordescription" title="ConnectorDescription">ConnectorDescription</a>: <i>String</i>
    <a href="#connectorname" title="ConnectorName">ConnectorName</a>: <i>String</i>
    <a href="#kafkacluster" title="KafkaCluster">KafkaCluster</a>: <i><a href="kafkacluster.md">KafkaCluster</a></i>
    <a href="#kafkaclusterclientauthentication" title="KafkaClusterClientAuthentication">KafkaClusterClientAuthentication</a>: <i><a href="kafkaclusterclientauthentication.md">KafkaClusterClientAuthentication</a></i>
    <a href="#kafkaclusterencryptionintransit" title="KafkaClusterEncryptionInTransit">KafkaClusterEncryptionInTransit</a>: <i><a href="kafkaclusterencryptionintransit.md">KafkaClusterEncryptionInTransit</a></i>
    <a href="#kafkaconnectversion" title="KafkaConnectVersion">KafkaConnectVersion</a>: <i>String</i>
    <a href="#logdelivery" title="LogDelivery">LogDelivery</a>: <i><a href="logdelivery.md">LogDelivery</a></i>
    <a href="#plugins" title="Plugins">Plugins</a>: <i>
      - <a href="plugin.md">Plugin</a></i>
    <a href="#serviceexecutionrolearn" title="ServiceExecutionRoleArn">ServiceExecutionRoleArn</a>: <i>String</i>
    <a href="#workerconfiguration" title="WorkerConfiguration">WorkerConfiguration</a>: <i><a href="workerconfiguration.md">WorkerConfiguration</a></i>
</pre>

## Properties

#### Capacity

Information about the capacity allocated to the connector.

_Required_: Yes

_Type_: <a href="capacity.md">Capacity</a>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### ConnectorConfiguration

The configuration for the connector.

_Required_: Yes

_Type_: <a href="connectorconfiguration.md">ConnectorConfiguration</a>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### ConnectorDescription

A summary description of the connector.

_Required_: No

_Type_: String

_Maximum Length_: <code>1024</code>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### ConnectorName

The name of the connector.

_Required_: Yes

_Type_: String

_Minimum Length_: <code>1</code>

_Maximum Length_: <code>128</code>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### KafkaCluster

Details of how to connect to the Kafka cluster.

_Required_: Yes

_Type_: <a href="kafkacluster.md">KafkaCluster</a>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### KafkaClusterClientAuthentication

Details of the client authentication used by the Kafka cluster.

_Required_: Yes

_Type_: <a href="kafkaclusterclientauthentication.md">KafkaClusterClientAuthentication</a>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### KafkaClusterEncryptionInTransit

Details of encryption in transit to the Kafka cluster.

_Required_: Yes

_Type_: <a href="kafkaclusterencryptionintransit.md">KafkaClusterEncryptionInTransit</a>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### KafkaConnectVersion

The version of Kafka Connect. It has to be compatible with both the Kafka cluster's version and the plugins.

_Required_: Yes

_Type_: String

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### LogDelivery

Details of what logs are delivered and where they are delivered.

_Required_: No

_Type_: <a href="logdelivery.md">LogDelivery</a>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### Plugins

List of plugins to use with the connector.

_Required_: Yes

_Type_: List of <a href="plugin.md">Plugin</a>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### ServiceExecutionRoleArn

The Amazon Resource Name (ARN) of the IAM role used by the connector to access Amazon S3 objects and other external resources.

_Required_: Yes

_Type_: String

_Pattern_: <code>arn:(aws|aws-us-gov|aws-cn):iam:.*</code>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### WorkerConfiguration

Specifies the worker configuration to use with the connector.

_Required_: No

_Type_: <a href="workerconfiguration.md">WorkerConfiguration</a>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

## Return Values

### Ref

When you pass the logical ID of this resource to the intrinsic `Ref` function, Ref returns the ConnectorArn.

### Fn::GetAtt

The `Fn::GetAtt` intrinsic function returns a value for a specified attribute of this type. The following are the available attributes and sample return values.

For more information about using the `Fn::GetAtt` intrinsic function, see [Fn::GetAtt](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/intrinsic-function-reference-getatt.html).

#### ConnectorArn

Amazon Resource Name for the created Connector.

