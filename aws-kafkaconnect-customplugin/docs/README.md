# AWS::KafkaConnect::CustomPlugin

An example resource schema demonstrating some basic constructs and validation rules.

## Syntax

To declare this entity in your AWS CloudFormation template, use the following syntax:

### JSON

<pre>
{
    "Type" : "AWS::KafkaConnect::CustomPlugin",
    "Properties" : {
        "<a href="#name" title="Name">Name</a>" : <i>String</i>,
        "<a href="#description" title="Description">Description</a>" : <i>String</i>,
        "<a href="#contenttype" title="ContentType">ContentType</a>" : <i>String</i>,
        "<a href="#filedescription" title="FileDescription">FileDescription</a>" : <i><a href="custompluginfiledescription.md">CustomPluginFileDescription</a></i>,
        "<a href="#location" title="Location">Location</a>" : <i><a href="custompluginlocation.md">CustomPluginLocation</a></i>,
        "<a href="#tags" title="Tags">Tags</a>" : <i>[ <a href="tag.md">Tag</a>, ... ]</i>
    }
}
</pre>

### YAML

<pre>
Type: AWS::KafkaConnect::CustomPlugin
Properties:
    <a href="#name" title="Name">Name</a>: <i>String</i>
    <a href="#description" title="Description">Description</a>: <i>String</i>
    <a href="#contenttype" title="ContentType">ContentType</a>: <i>String</i>
    <a href="#filedescription" title="FileDescription">FileDescription</a>: <i><a href="custompluginfiledescription.md">CustomPluginFileDescription</a></i>
    <a href="#location" title="Location">Location</a>: <i><a href="custompluginlocation.md">CustomPluginLocation</a></i>
    <a href="#tags" title="Tags">Tags</a>: <i>
      - <a href="tag.md">Tag</a></i>
</pre>

## Properties

#### Name

The name of the custom plugin.

_Required_: Yes

_Type_: String

_Minimum Length_: <code>1</code>

_Maximum Length_: <code>128</code>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### Description

A summary description of the custom plugin.

_Required_: No

_Type_: String

_Maximum Length_: <code>1024</code>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### ContentType

The type of the plugin file.

_Required_: Yes

_Type_: String

_Allowed Values_: <code>JAR</code> | <code>ZIP</code>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### FileDescription

Details about the custom plugin file.

_Required_: No

_Type_: <a href="custompluginfiledescription.md">CustomPluginFileDescription</a>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### Location

Information about the location of a custom plugin.

_Required_: Yes

_Type_: <a href="custompluginlocation.md">CustomPluginLocation</a>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### Tags

An array of key-value pairs to apply to this resource.

_Required_: No

_Type_: List of <a href="tag.md">Tag</a>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

## Return Values

### Ref

When you pass the logical ID of this resource to the intrinsic `Ref` function, Ref returns the CustomPluginArn.

### Fn::GetAtt

The `Fn::GetAtt` intrinsic function returns a value for a specified attribute of this type. The following are the available attributes and sample return values.

For more information about using the `Fn::GetAtt` intrinsic function, see [Fn::GetAtt](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/intrinsic-function-reference-getatt.html).

#### CustomPluginArn

The Amazon Resource Name (ARN) of the custom plugin to use.

#### Revision

The revision of the custom plugin.

#### FileDescription

Details about the custom plugin file.
