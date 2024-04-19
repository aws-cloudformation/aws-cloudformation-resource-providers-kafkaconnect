# AWS::KafkaConnect::Connector AutoScaling

Details about auto scaling of a connector.

## Syntax

To declare this entity in your AWS CloudFormation template, use the following syntax:

### JSON

<pre>
{
    "<a href="#maxworkercount" title="MaxWorkerCount">MaxWorkerCount</a>" : <i>Integer</i>,
    "<a href="#minworkercount" title="MinWorkerCount">MinWorkerCount</a>" : <i>Integer</i>,
    "<a href="#scaleinpolicy" title="ScaleInPolicy">ScaleInPolicy</a>" : <i><a href="scaleinpolicy.md">ScaleInPolicy</a></i>,
    "<a href="#scaleoutpolicy" title="ScaleOutPolicy">ScaleOutPolicy</a>" : <i><a href="scaleoutpolicy.md">ScaleOutPolicy</a></i>,
    "<a href="#mcucount" title="McuCount">McuCount</a>" : <i>Integer</i>
}
</pre>

### YAML

<pre>
<a href="#maxworkercount" title="MaxWorkerCount">MaxWorkerCount</a>: <i>Integer</i>
<a href="#minworkercount" title="MinWorkerCount">MinWorkerCount</a>: <i>Integer</i>
<a href="#scaleinpolicy" title="ScaleInPolicy">ScaleInPolicy</a>: <i><a href="scaleinpolicy.md">ScaleInPolicy</a></i>
<a href="#scaleoutpolicy" title="ScaleOutPolicy">ScaleOutPolicy</a>: <i><a href="scaleoutpolicy.md">ScaleOutPolicy</a></i>
<a href="#mcucount" title="McuCount">McuCount</a>: <i>Integer</i>
</pre>

## Properties

#### MaxWorkerCount

The maximum number of workers for a connector.

_Required_: Yes

_Type_: Integer

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### MinWorkerCount

The minimum number of workers for a connector.

_Required_: Yes

_Type_: Integer

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### ScaleInPolicy

Information about the scale in policy of the connector.

_Required_: Yes

_Type_: <a href="scaleinpolicy.md">ScaleInPolicy</a>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### ScaleOutPolicy

Information about the scale out policy of the connector.

_Required_: Yes

_Type_: <a href="scaleoutpolicy.md">ScaleOutPolicy</a>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### McuCount

Specifies how many MSK Connect Units (MCU) as the minimum scaling unit.

_Required_: Yes

_Type_: Integer

_Allowed Values_: <code>1</code> | <code>2</code> | <code>4</code> | <code>8</code>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)
