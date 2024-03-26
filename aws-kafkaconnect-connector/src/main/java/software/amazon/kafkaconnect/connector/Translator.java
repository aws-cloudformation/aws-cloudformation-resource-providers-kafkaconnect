package software.amazon.kafkaconnect.connector;

import software.amazon.awssdk.services.kafkaconnect.model.ApacheKafkaClusterDescription;
import software.amazon.awssdk.services.kafkaconnect.model.AutoScalingDescription;
import software.amazon.awssdk.services.kafkaconnect.model.CapacityDescription;
import software.amazon.awssdk.services.kafkaconnect.model.AutoScalingUpdate;
import software.amazon.awssdk.services.kafkaconnect.model.CapacityUpdate;
import software.amazon.awssdk.services.kafkaconnect.model.CloudWatchLogsLogDeliveryDescription;
import software.amazon.awssdk.services.kafkaconnect.model.CreateConnectorRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DeleteConnectorRequest;
import software.amazon.awssdk.services.kafkaconnect.model.CustomPluginDescription;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeConnectorRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeConnectorResponse;
import software.amazon.awssdk.services.kafkaconnect.model.FirehoseLogDeliveryDescription;
import software.amazon.awssdk.services.kafkaconnect.model.KafkaClusterClientAuthenticationDescription;
import software.amazon.awssdk.services.kafkaconnect.model.KafkaClusterDescription;
import software.amazon.awssdk.services.kafkaconnect.model.KafkaClusterEncryptionInTransitDescription;
import software.amazon.awssdk.services.kafkaconnect.model.ListConnectorsRequest;
import software.amazon.awssdk.services.kafkaconnect.model.ListConnectorsResponse;
import software.amazon.awssdk.services.kafkaconnect.model.LogDeliveryDescription;
import software.amazon.awssdk.services.kafkaconnect.model.PluginDescription;
import software.amazon.awssdk.services.kafkaconnect.model.ProvisionedCapacityUpdate;
import software.amazon.awssdk.services.kafkaconnect.model.S3LogDeliveryDescription;
import software.amazon.awssdk.services.kafkaconnect.model.ScaleInPolicyDescription;
import software.amazon.awssdk.services.kafkaconnect.model.ScaleInPolicyUpdate;
import software.amazon.awssdk.services.kafkaconnect.model.ScaleOutPolicyDescription;
import software.amazon.awssdk.services.kafkaconnect.model.ScaleOutPolicyUpdate;
import software.amazon.awssdk.services.kafkaconnect.model.TagResourceRequest;
import software.amazon.awssdk.services.kafkaconnect.model.UntagResourceRequest;
import software.amazon.awssdk.services.kafkaconnect.model.UpdateConnectorRequest;
import software.amazon.awssdk.services.kafkaconnect.model.VpcDescription;
import software.amazon.awssdk.services.kafkaconnect.model.WorkerConfigurationDescription;
import software.amazon.awssdk.services.kafkaconnect.model.WorkerLogDeliveryDescription;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class is a centralized placeholder for
 *  - api request construction
 *  - object translation to/from aws sdk
 *  - resource model construction for read/list handlers
 */

public class Translator {

    public Translator() {
    }

    /**
     * Request to create a resource
     * @param model resource model
     * @return createConnectorRequest the kafkaconnect request to create a resource
     */
    public CreateConnectorRequest translateToCreateRequest(final ResourceModel model,
        final Map<String, String> tagsForCreate) {
        return CreateConnectorRequest.builder()
            .capacity(resourceCapacityToSdkCapacity(model.getCapacity()))
            .connectorConfiguration(model.getConnectorConfiguration())
            .connectorDescription(model.getConnectorDescription())
            .connectorName(model.getConnectorName())
            .kafkaCluster(resourceKafkaClusterToSdkKafkaCluster(model.getKafkaCluster()))
            .kafkaClusterClientAuthentication(resourceKafkaClusterClientAuthToSdkKafkaClusterClientAuth(
                model.getKafkaClusterClientAuthentication()))
            .kafkaClusterEncryptionInTransit(resourceKafkaClusterEITToSdkKafkaClusterEIT(
                model.getKafkaClusterEncryptionInTransit()))
            .kafkaConnectVersion(model.getKafkaConnectVersion())
            .plugins(model.getPlugins().stream()
                .map(plugin -> resourcePluginToSdkPlugin(plugin))
                .collect(Collectors.toList()))
            .logDelivery(resourceLogDeliveryToSdkLogDelivery(model.getLogDelivery()))
            .serviceExecutionRoleArn(model.getServiceExecutionRoleArn())
            .workerConfiguration(resourceWorkerConfigurationToSdkWorkerConfiguration(model.getWorkerConfiguration()))
            .tags(tagsForCreate)
            .build();
    }

    /**
     * Request to read a resource
     * @param model resource model
     * @return describeConnectorRequest the kafkaconnect request to describe a resource
     */
    public DescribeConnectorRequest translateToReadRequest(final ResourceModel model) {
        return DescribeConnectorRequest.builder()
            .connectorArn(model.getConnectorArn())
            .build();
    }

    /**
     * Translates resource object from sdk into a resource model
     * @param describeConnectorResponse the kafkaconnect describe resource response
     * @return model resource model
     */
    public ResourceModel translateFromReadResponse(final DescribeConnectorResponse describeConnectorResponse) {
        return ResourceModel
            .builder()
            .capacity(sdkCapacityDescriptionToResourceCapacity(describeConnectorResponse.capacity()))
            .connectorArn(describeConnectorResponse.connectorArn())
            .connectorConfiguration(describeConnectorResponse.connectorConfiguration())
            .connectorDescription(describeConnectorResponse.connectorDescription())
            .connectorName(describeConnectorResponse.connectorName())
            .kafkaCluster(sdkKafkaClusterDescriptionToResourceKafkaCluster(describeConnectorResponse.kafkaCluster()))
            .kafkaClusterClientAuthentication(sdkKafkaClusterClientAuthDescriptionToResourceKafkaClusterClientAuth(
                describeConnectorResponse.kafkaClusterClientAuthentication()))
            .kafkaClusterEncryptionInTransit(sdkKafkaClusterEITDescriptionToResourceKafkaClusterEIT(
                describeConnectorResponse.kafkaClusterEncryptionInTransit()))
            .kafkaConnectVersion(describeConnectorResponse.kafkaConnectVersion())
            .logDelivery(sdkLogDeliveryDescriptionToResourceLogDelivery(describeConnectorResponse.logDelivery()))
            .plugins(describeConnectorResponse.plugins().stream()
                .map(pluginDescription -> sdkPluginDescriptionToResourcePlugin(pluginDescription))
                .collect(Collectors.toSet()))
            .serviceExecutionRoleArn(describeConnectorResponse.serviceExecutionRoleArn())
            .workerConfiguration(
                sdkWorkerConfigurationDescriptionToResourceWorkerConfiguration(
                    describeConnectorResponse.workerConfiguration()))
            .build();
    }

    /**
     * Request to delete a resource
     * @param model resource model
     * @return deleteConnectorRequest the kafkaconnect request to delete a resource
     */
    public DeleteConnectorRequest translateToDeleteRequest(final ResourceModel model) {
        return DeleteConnectorRequest.builder()
            .connectorArn(model.getConnectorArn())
            .build();
    }

    /**
     * Request to list resources
     * @param nextToken token passed to the aws service list resources request
     * @return listConnectorsRequest the kafkaconnect request to list resources within aws account
     */
    public ListConnectorsRequest translateToListRequest(final String nextToken) {
        return ListConnectorsRequest.builder()
            .nextToken(nextToken)
            .build();
    }

    /**
     * Translates connectors from sdk into a resource model (primary identifier only)
     * @param listConnectorsResponse the kafkaconnect list resources response
     * @return list of resource models
     */
    public List<ResourceModel> translateFromListResponse(final ListConnectorsResponse listConnectorsResponse) {
        return streamOfOrEmpty(listConnectorsResponse.connectors())
            .map(connector -> ResourceModel.builder()
                .capacity(sdkCapacityDescriptionToResourceCapacity(connector.capacity()))
                .connectorArn(connector.connectorArn())
                .connectorDescription(connector.connectorDescription())
                .connectorName(connector.connectorName())
                .kafkaCluster(sdkKafkaClusterDescriptionToResourceKafkaCluster(connector.kafkaCluster()))
                .kafkaClusterClientAuthentication(
                    sdkKafkaClusterClientAuthDescriptionToResourceKafkaClusterClientAuth(
                        connector.kafkaClusterClientAuthentication()))
                .kafkaClusterEncryptionInTransit(sdkKafkaClusterEITDescriptionToResourceKafkaClusterEIT(
                    connector.kafkaClusterEncryptionInTransit()))
                .kafkaConnectVersion(connector.kafkaConnectVersion())
                .logDelivery(sdkLogDeliveryDescriptionToResourceLogDelivery(connector.logDelivery()))
                .plugins(connector.plugins().stream()
                    .map(pluginDescription -> sdkPluginDescriptionToResourcePlugin(pluginDescription))
                    .collect(Collectors.toSet()))
                .serviceExecutionRoleArn(connector.serviceExecutionRoleArn())
                .workerConfiguration(sdkWorkerConfigurationDescriptionToResourceWorkerConfiguration(
                    connector.workerConfiguration()))
                .build())
            .collect(Collectors.toList());
    }

    /**
     * Request to update properties of a previously created resource
     * @param model resource model
     * @return updateConnectorRequest the kafkaconnect request to modify a resource
     */
    public UpdateConnectorRequest translateToUpdateRequest(final ResourceModel model) {
        return UpdateConnectorRequest.builder()
            .capacity(resourceCapacityToSdkCapacityUpdate(model.getCapacity()))
            .connectorArn(model.getConnectorArn())
            .build();
    }

    private static <T> Stream<T> streamOfOrEmpty(final Collection<T> collection) {
        return Optional.ofNullable(collection)
            .map(Collection::stream)
            .orElseGet(Stream::empty);
    }

    private static software.amazon.awssdk.services.kafkaconnect.model.Plugin resourcePluginToSdkPlugin(
        final Plugin plugin) {

        return plugin == null ? null : software.amazon.awssdk.services.kafkaconnect.model.Plugin.builder()
            .customPlugin(resourceCustomPluginToSdkCustomPlugin(plugin.getCustomPlugin()))
            .build();
    }

    private static Plugin sdkPluginDescriptionToResourcePlugin(final PluginDescription pluginDescription) {
        return pluginDescription == null ? null : Plugin.builder()
            .customPlugin(sdkCustomPluginDescriptionToResourceCustomPlugin(pluginDescription.customPlugin()))
            .build();
    }

    private static software.amazon.awssdk.services.kafkaconnect.model.CustomPlugin
        resourceCustomPluginToSdkCustomPlugin(final CustomPlugin customPlugin) {

        return customPlugin == null ? null :
            software.amazon.awssdk.services.kafkaconnect.model.CustomPlugin.builder()
                .customPluginArn(customPlugin.getCustomPluginArn())
                .revision(customPlugin.getRevision())
                .build();
    }

    private static CustomPlugin sdkCustomPluginDescriptionToResourceCustomPlugin(
        final CustomPluginDescription customPluginDescription) {

        return customPluginDescription == null ? null : CustomPlugin.builder()
            .customPluginArn(customPluginDescription.customPluginArn())
            .revision(customPluginDescription.revision())
            .build();
    }

    private static software.amazon.awssdk.services.kafkaconnect.model.Capacity resourceCapacityToSdkCapacity(
        final Capacity capacity) {

        return capacity == null ? null : software.amazon.awssdk.services.kafkaconnect.model.Capacity.builder()
            .provisionedCapacity(resourceProvisionedCapacityToSdkProvisionedCapacity(
                capacity.getProvisionedCapacity()))
            .autoScaling(resourceAutoScalingToSdkAutoScaling(capacity.getAutoScaling()))
            .build();
    }

    private static CapacityUpdate resourceCapacityToSdkCapacityUpdate(final Capacity capacity) {
        return capacity == null ? null : CapacityUpdate.builder()
            .autoScaling(resourceAutoScalingToSdkAutoScalingUpdate(capacity.getAutoScaling()))
            .provisionedCapacity(resourceProvisionedCapacityToSdkProvisionedCapacityUpdate(
                capacity.getProvisionedCapacity()))
            .build();
    }

    private static Capacity sdkCapacityDescriptionToResourceCapacity(final CapacityDescription capacityDescription) {
        return capacityDescription == null ? null : Capacity.builder()
            .provisionedCapacity(sdkProvisionedCapacityDescriptionToResourceProvisionedCapacity(
                capacityDescription.provisionedCapacity()))
            .autoScaling(sdkAutoScalingDescriptionToResourceAutoScaling(capacityDescription.autoScaling()))
            .build();
    }

    private static software.amazon.awssdk.services.kafkaconnect.model.AutoScaling
        resourceAutoScalingToSdkAutoScaling(final AutoScaling autoScaling) {

        return autoScaling == null ? null :
            software.amazon.awssdk.services.kafkaconnect.model.AutoScaling.builder()
            .maxWorkerCount(autoScaling.getMaxWorkerCount())
            .minWorkerCount(autoScaling.getMinWorkerCount())
            .scaleInPolicy(resourceScaleInPolicyToSdkScaleInPolicy(autoScaling.getScaleInPolicy()))
            .scaleOutPolicy(resourceScaleOutPolicyToSdkScaleOutPolicy(autoScaling.getScaleOutPolicy()))
            .mcuCount(autoScaling.getMcuCount())
            .build();
    }

    private static AutoScalingUpdate resourceAutoScalingToSdkAutoScalingUpdate(final AutoScaling autoScaling) {
        return autoScaling == null ? null : AutoScalingUpdate.builder()
            .maxWorkerCount(autoScaling.getMaxWorkerCount())
            .minWorkerCount(autoScaling.getMinWorkerCount())
            .scaleInPolicy(resourceScaleInPolicyToSdkScaleInPolicyUpdate(autoScaling.getScaleInPolicy()))
            .scaleOutPolicy(resourceScaleOutPolicyToSdkScaleOutPolicyUpdate(autoScaling.getScaleOutPolicy()))
            .mcuCount(autoScaling.getMcuCount())
            .build();
    }

    private static AutoScaling sdkAutoScalingDescriptionToResourceAutoScaling(
        final AutoScalingDescription autoScalingDescription) {

        return autoScalingDescription == null ? null : AutoScaling.builder()
            .maxWorkerCount(autoScalingDescription.maxWorkerCount())
            .minWorkerCount(autoScalingDescription.minWorkerCount())
            .scaleInPolicy(sdkScaleInPolicyDescriptionToResourceScaleInPolicy(autoScalingDescription.scaleInPolicy()))
            .scaleOutPolicy(sdkScaleOutPolicyDescriptionToResourceScaleOutPolicy(autoScalingDescription.scaleOutPolicy()))
            .mcuCount(autoScalingDescription.mcuCount())
            .build();
    }

    private static software.amazon.awssdk.services.kafkaconnect.model.ScaleInPolicy
        resourceScaleInPolicyToSdkScaleInPolicy(final ScaleInPolicy scaleInPolicy) {

        return scaleInPolicy == null ? null :
            software.amazon.awssdk.services.kafkaconnect.model.ScaleInPolicy.builder()
                .cpuUtilizationPercentage(scaleInPolicy.getCpuUtilizationPercentage())
                .build();
    }

    private static ScaleInPolicyUpdate resourceScaleInPolicyToSdkScaleInPolicyUpdate(
        final ScaleInPolicy scaleInPolicy) {

        return scaleInPolicy == null ? null : ScaleInPolicyUpdate.builder()
            .cpuUtilizationPercentage(scaleInPolicy.getCpuUtilizationPercentage())
            .build();
    }

    private static ScaleInPolicy sdkScaleInPolicyDescriptionToResourceScaleInPolicy(
        final ScaleInPolicyDescription scaleInPolicy) {

        return scaleInPolicy == null ? null : ScaleInPolicy.builder()
            .cpuUtilizationPercentage(scaleInPolicy.cpuUtilizationPercentage())
            .build();
    }

    private static software.amazon.awssdk.services.kafkaconnect.model.ScaleOutPolicy
        resourceScaleOutPolicyToSdkScaleOutPolicy(final ScaleOutPolicy scaleOutPolicy) {

        return scaleOutPolicy == null ? null :
            software.amazon.awssdk.services.kafkaconnect.model.ScaleOutPolicy.builder()
                .cpuUtilizationPercentage(scaleOutPolicy.getCpuUtilizationPercentage())
                .build();
    }

    private static ScaleOutPolicyUpdate resourceScaleOutPolicyToSdkScaleOutPolicyUpdate(
        final ScaleOutPolicy scaleOutPolicy) {

        return scaleOutPolicy == null ? null : ScaleOutPolicyUpdate.builder()
            .cpuUtilizationPercentage(scaleOutPolicy.getCpuUtilizationPercentage())
            .build();
    }

    private static ScaleOutPolicy sdkScaleOutPolicyDescriptionToResourceScaleOutPolicy(
        final ScaleOutPolicyDescription scaleOutPolicy) {

        return scaleOutPolicy == null ? null : ScaleOutPolicy.builder()
            .cpuUtilizationPercentage(scaleOutPolicy.cpuUtilizationPercentage())
            .build();
    }

    private static software.amazon.awssdk.services.kafkaconnect.model.ProvisionedCapacity
        resourceProvisionedCapacityToSdkProvisionedCapacity(final ProvisionedCapacity provisionedCapacity) {

        return provisionedCapacity == null ? null :
            software.amazon.awssdk.services.kafkaconnect.model.ProvisionedCapacity.builder()
                .workerCount(provisionedCapacity.getWorkerCount())
                .mcuCount(provisionedCapacity.getMcuCount())
                .build();
    }

    private static ProvisionedCapacityUpdate resourceProvisionedCapacityToSdkProvisionedCapacityUpdate(
        final ProvisionedCapacity provisionedCapacity) {

        return provisionedCapacity == null ? null : ProvisionedCapacityUpdate.builder()
            .workerCount(provisionedCapacity.getWorkerCount())
            .mcuCount(provisionedCapacity.getMcuCount())
            .build();
    }

    private static ProvisionedCapacity sdkProvisionedCapacityDescriptionToResourceProvisionedCapacity(
        final software.amazon.awssdk.services.kafkaconnect.model.ProvisionedCapacityDescription
            provisionedCapacityDescription) {

        return provisionedCapacityDescription == null ? null : ProvisionedCapacity.builder()
            .mcuCount(provisionedCapacityDescription.mcuCount())
            .workerCount(provisionedCapacityDescription.workerCount())
            .build();
    }

    private static software.amazon.awssdk.services.kafkaconnect.model.KafkaCluster
        resourceKafkaClusterToSdkKafkaCluster(final KafkaCluster kafkaCluster) {

        return kafkaCluster == null ? null :
            software.amazon.awssdk.services.kafkaconnect.model.KafkaCluster.builder()
                .apacheKafkaCluster(resourceApacheKafkaClusterToSdkApacheKafkaCluster(
                    kafkaCluster.getApacheKafkaCluster()))
                .build();
    }

    private static KafkaCluster sdkKafkaClusterDescriptionToResourceKafkaCluster(
        final KafkaClusterDescription kafkaClusterDescription) {

        return kafkaClusterDescription == null ? null : KafkaCluster.builder()
            .apacheKafkaCluster(sdkApacheKafkaClusterDescriptionToResourceApacheKafkaCluster(
                kafkaClusterDescription.apacheKafkaCluster()))
            .build();
    }

    private static software.amazon.awssdk.services.kafkaconnect.model.ApacheKafkaCluster
        resourceApacheKafkaClusterToSdkApacheKafkaCluster(final ApacheKafkaCluster apacheKafkaCluster) {

        return apacheKafkaCluster == null ? null :
            software.amazon.awssdk.services.kafkaconnect.model.ApacheKafkaCluster.builder()
                .bootstrapServers(apacheKafkaCluster.getBootstrapServers())
                .vpc(resourceVpcToSdkVpc(apacheKafkaCluster.getVpc()))
                .build();
    }

    private static ApacheKafkaCluster sdkApacheKafkaClusterDescriptionToResourceApacheKafkaCluster(
        final ApacheKafkaClusterDescription apacheKafkaClusterDescription) {

        return apacheKafkaClusterDescription == null ? null : ApacheKafkaCluster.builder()
            .bootstrapServers(apacheKafkaClusterDescription.bootstrapServers())
            .vpc(sdkVpcDescriptionToResourceVpc(apacheKafkaClusterDescription.vpc()))
            .build();
    }

    private static software.amazon.awssdk.services.kafkaconnect.model.KafkaClusterClientAuthentication
        resourceKafkaClusterClientAuthToSdkKafkaClusterClientAuth(
            final KafkaClusterClientAuthentication kafkaClusterClientAuthentication) {
        return kafkaClusterClientAuthentication == null ? null :
            software.amazon.awssdk.services.kafkaconnect.model.KafkaClusterClientAuthentication.builder()
                .authenticationType(kafkaClusterClientAuthentication.getAuthenticationType())
                .build();
    }

    private static KafkaClusterClientAuthentication
        sdkKafkaClusterClientAuthDescriptionToResourceKafkaClusterClientAuth(
            final KafkaClusterClientAuthenticationDescription kafkaClusterClientAuthenticationDescription) {
        return kafkaClusterClientAuthenticationDescription == null ? null : KafkaClusterClientAuthentication.builder()
            .authenticationType(kafkaClusterClientAuthenticationDescription.authenticationTypeAsString())
            .build();
    }

    private static software.amazon.awssdk.services.kafkaconnect.model.KafkaClusterEncryptionInTransit
        resourceKafkaClusterEITToSdkKafkaClusterEIT(
            final KafkaClusterEncryptionInTransit kafkaClusterEncryptionInTransit) {
        return kafkaClusterEncryptionInTransit == null ? null :
            software.amazon.awssdk.services.kafkaconnect.model.KafkaClusterEncryptionInTransit.builder()
                .encryptionType(kafkaClusterEncryptionInTransit.getEncryptionType())
                .build();
    }

    private static KafkaClusterEncryptionInTransit sdkKafkaClusterEITDescriptionToResourceKafkaClusterEIT(
        final KafkaClusterEncryptionInTransitDescription kafkaClusterEncryptionInTransitDescription) {

        return kafkaClusterEncryptionInTransitDescription == null ? null : KafkaClusterEncryptionInTransit.builder()
            .encryptionType(kafkaClusterEncryptionInTransitDescription.encryptionTypeAsString())
            .build();
    }

    private static software.amazon.awssdk.services.kafkaconnect.model.Vpc resourceVpcToSdkVpc(final Vpc vpc) {
        return vpc == null ? null :
            software.amazon.awssdk.services.kafkaconnect.model.Vpc.builder()
                .securityGroups(vpc.getSecurityGroups())
                .subnets(vpc.getSubnets())
                .build();
    }

    private static Vpc sdkVpcDescriptionToResourceVpc(final VpcDescription vpcDescription) {
        return vpcDescription == null ? null : Vpc.builder()
            .securityGroups(new HashSet<>(vpcDescription.securityGroups()))
            .subnets(new HashSet<>(vpcDescription.subnets()))
            .build();
    }

    private static software.amazon.awssdk.services.kafkaconnect.model.LogDelivery
        resourceLogDeliveryToSdkLogDelivery(final LogDelivery logDelivery) {

        return logDelivery == null ? null :
            software.amazon.awssdk.services.kafkaconnect.model.LogDelivery.builder()
                .workerLogDelivery(resourceWorkerLogDeliveryToSdkWorkerLogDelivery(
                    logDelivery.getWorkerLogDelivery()))
                .build();
    }

    private static LogDelivery sdkLogDeliveryDescriptionToResourceLogDelivery(
        final LogDeliveryDescription logDeliveryDescription) {

        return logDeliveryDescription == null ? null : LogDelivery.builder()
            .workerLogDelivery(sdkWorkerLogDeliveryDescriptionToResourceWorkerLogDelivery(
                logDeliveryDescription.workerLogDelivery()))
            .build();
    }

    private static software.amazon.awssdk.services.kafkaconnect.model.WorkerLogDelivery
        resourceWorkerLogDeliveryToSdkWorkerLogDelivery(final WorkerLogDelivery workerLogDelivery) {

        return workerLogDelivery == null ? null :
            software.amazon.awssdk.services.kafkaconnect.model.WorkerLogDelivery.builder()
                .cloudWatchLogs(resourceCloudWatchLogsLogDeliveryToSdkCloudWatchLogsLogDelivery(
                    workerLogDelivery.getCloudWatchLogs()))
                .firehose(resourceFirehoseLogDeliveryToSdkFirehoseLogDelivery(workerLogDelivery.getFirehose()))
                .s3(resourceS3LogDeliveryToSdkS3LogDelivery(workerLogDelivery.getS3()))
                .build();
    }

    private static WorkerLogDelivery sdkWorkerLogDeliveryDescriptionToResourceWorkerLogDelivery(
        final WorkerLogDeliveryDescription workerLogDeliveryDescription) {

        return workerLogDeliveryDescription == null ? null : WorkerLogDelivery.builder()
            .cloudWatchLogs(sdkCloudWatchLogsLogDeliveryDescriptionToResourceCloudWatchLogsLogDelivery(
                workerLogDeliveryDescription.cloudWatchLogs()))
            .firehose(sdkFirehoseLogDeliveryDescriptionToResourceFirehoseLogDelivery(
                workerLogDeliveryDescription.firehose()))
            .s3(sdkS3LogDeliveryDescriptionToResourceS3LogDelivery(workerLogDeliveryDescription.s3()))
            .build();
    }

    private static software.amazon.awssdk.services.kafkaconnect.model.CloudWatchLogsLogDelivery
        resourceCloudWatchLogsLogDeliveryToSdkCloudWatchLogsLogDelivery(
            final CloudWatchLogsLogDelivery cloudWatchLogsLogDelivery) {

        return cloudWatchLogsLogDelivery == null ? null :
            software.amazon.awssdk.services.kafkaconnect.model.CloudWatchLogsLogDelivery.builder()
                .logGroup(cloudWatchLogsLogDelivery.getLogGroup())
                .enabled(cloudWatchLogsLogDelivery.getEnabled())
                .build();
    }

    private static CloudWatchLogsLogDelivery
        sdkCloudWatchLogsLogDeliveryDescriptionToResourceCloudWatchLogsLogDelivery(
            final CloudWatchLogsLogDeliveryDescription cloudWatchLogsLogDeliveryDescription) {

        return cloudWatchLogsLogDeliveryDescription == null ? null : CloudWatchLogsLogDelivery.builder()
            .logGroup(cloudWatchLogsLogDeliveryDescription.logGroup())
            .enabled(cloudWatchLogsLogDeliveryDescription.enabled())
            .build();
    }

    private static software.amazon.awssdk.services.kafkaconnect.model.FirehoseLogDelivery
        resourceFirehoseLogDeliveryToSdkFirehoseLogDelivery(final FirehoseLogDelivery firehoseLogDelivery) {

        return firehoseLogDelivery == null ? null :
            software.amazon.awssdk.services.kafkaconnect.model.FirehoseLogDelivery.builder()
                .deliveryStream(firehoseLogDelivery.getDeliveryStream())
                .enabled(firehoseLogDelivery.getEnabled())
                .build();
    }

    private static FirehoseLogDelivery sdkFirehoseLogDeliveryDescriptionToResourceFirehoseLogDelivery(
        final FirehoseLogDeliveryDescription firehoseLogDeliveryDescription) {

        return firehoseLogDeliveryDescription == null ? null : FirehoseLogDelivery.builder()
            .deliveryStream(firehoseLogDeliveryDescription.deliveryStream())
            .enabled(firehoseLogDeliveryDescription.enabled())
            .build();
    }

    private static software.amazon.awssdk.services.kafkaconnect.model.S3LogDelivery
        resourceS3LogDeliveryToSdkS3LogDelivery(final S3LogDelivery s3LogDelivery) {

        return s3LogDelivery == null ? null :
            software.amazon.awssdk.services.kafkaconnect.model.S3LogDelivery.builder()
                .bucket(s3LogDelivery.getBucket())
                .prefix(s3LogDelivery.getPrefix())
                .enabled(s3LogDelivery.getEnabled())
                .build();
    }

    private static S3LogDelivery sdkS3LogDeliveryDescriptionToResourceS3LogDelivery(
        final S3LogDeliveryDescription s3LogDeliveryDescription) {
        return s3LogDeliveryDescription == null ? null : S3LogDelivery.builder()
            .bucket(s3LogDeliveryDescription.bucket())
            .prefix(s3LogDeliveryDescription.prefix())
            .enabled(s3LogDeliveryDescription.enabled())
            .build();
    }

    private static software.amazon.awssdk.services.kafkaconnect.model.WorkerConfiguration
        resourceWorkerConfigurationToSdkWorkerConfiguration(final WorkerConfiguration workerConfiguration) {

        return workerConfiguration == null ? null :
            software.amazon.awssdk.services.kafkaconnect.model.WorkerConfiguration.builder()
                .revision(workerConfiguration.getRevision())
                .workerConfigurationArn(workerConfiguration.getWorkerConfigurationArn())
                .build();
    }

    private static WorkerConfiguration sdkWorkerConfigurationDescriptionToResourceWorkerConfiguration(
        final WorkerConfigurationDescription workerConfigurationDescription) {

        return workerConfigurationDescription == null ? null : WorkerConfiguration.builder()
            .revision(workerConfigurationDescription.revision())
            .workerConfigurationArn(workerConfigurationDescription.workerConfigurationArn())
            .build();
    }

    /**
     * Request to add tags to a resource
     *
     * @param model resource model
     * @return awsRequest the aws service request to create a resource
     */
    static TagResourceRequest tagResourceRequest(final ResourceModel model, final Map<String, String> addedTags) {
        return TagResourceRequest.builder()
                .resourceArn(model.getConnectorArn())
                .tags(addedTags)
                .build();
    }

    /**
     * Request to add tags to a resource
     *
     * @param model resource model
     * @return awsRequest the aws service request to create a resource
     */
    static UntagResourceRequest untagResourceRequest(final ResourceModel model, final Set<String> removedTags) {
        return UntagResourceRequest.builder()
                .resourceArn(model.getConnectorArn())
                .tagKeys(removedTags)
                .build();

    }
}
