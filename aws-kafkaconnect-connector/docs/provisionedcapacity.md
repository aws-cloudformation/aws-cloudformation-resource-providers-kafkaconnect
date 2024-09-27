# AWS::KafkaConnect::Connector ProvisionedCapacity

Details about a fixed capacity allocated to a connector.

## Syntax

To declare this entity in your AWS CloudFormation template, use the following syntax:

### JSON

<pre>
{
    "<a href="#mcucount" title="McuCount">McuCount</a>" : <i>Integer</i>,
    "<a href="#workercount" title="WorkerCount">WorkerCount</a>" : <i>Integer</i>
}
</pre>

### YAML

<pre>
<a href="#mcucount" title="McuCount">McuCount</a>: <i>Integer</i>
<a href="#workercount" title="WorkerCount">WorkerCount</a>: <i>Integer</i>
</pre>

## Properties

#### McuCount

Specifies how many MSK Connect Units (MCU) are allocated to the connector.

_Required_: No

_Type_: Integer

_Allowed Values_: <code>1</code> | <code>2</code> | <code>4</code> | <code>8</code>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### WorkerCount

Number of workers for a connector.

_Required_: Yes

_Type_: Integer

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)
