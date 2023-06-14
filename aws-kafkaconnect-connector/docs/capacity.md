# AWS::KafkaConnect::Connector Capacity

Information about the capacity allocated to the connector.

## Syntax

To declare this entity in your AWS CloudFormation template, use the following syntax:

### JSON

<pre>
{
    "<a href="#autoscaling" title="AutoScaling">AutoScaling</a>" : <i><a href="autoscaling.md">AutoScaling</a></i>,
    "<a href="#provisionedcapacity" title="ProvisionedCapacity">ProvisionedCapacity</a>" : <i><a href="provisionedcapacity.md">ProvisionedCapacity</a></i>
}
</pre>

### YAML

<pre>
<a href="#autoscaling" title="AutoScaling">AutoScaling</a>: <i><a href="autoscaling.md">AutoScaling</a></i>
<a href="#provisionedcapacity" title="ProvisionedCapacity">ProvisionedCapacity</a>: <i><a href="provisionedcapacity.md">ProvisionedCapacity</a></i>
</pre>

## Properties

#### AutoScaling

Details about auto scaling of a connector. 

_Required_: Yes

_Type_: <a href="autoscaling.md">AutoScaling</a>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### ProvisionedCapacity

Details about a fixed capacity allocated to a connector.

_Required_: Yes

_Type_: <a href="provisionedcapacity.md">ProvisionedCapacity</a>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

