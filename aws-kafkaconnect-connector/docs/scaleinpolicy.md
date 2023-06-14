# AWS::KafkaConnect::Connector ScaleInPolicy

Information about the scale in policy of the connector.

## Syntax

To declare this entity in your AWS CloudFormation template, use the following syntax:

### JSON

<pre>
{
    "<a href="#cpuutilizationpercentage" title="CpuUtilizationPercentage">CpuUtilizationPercentage</a>" : <i>Integer</i>
}
</pre>

### YAML

<pre>
<a href="#cpuutilizationpercentage" title="CpuUtilizationPercentage">CpuUtilizationPercentage</a>: <i>Integer</i>
</pre>

## Properties

#### CpuUtilizationPercentage

Specifies the CPU utilization percentage threshold at which connector scale in should trigger.

_Required_: Yes

_Type_: Integer

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

