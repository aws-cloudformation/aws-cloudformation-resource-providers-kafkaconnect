package software.amazon.kafkaconnect.connector;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.kafkaconnect.model.ApacheKafkaClusterDescription;
import software.amazon.awssdk.services.kafkaconnect.model.AutoScalingDescription;
import software.amazon.awssdk.services.kafkaconnect.model.AutoScalingUpdate;
import software.amazon.awssdk.services.kafkaconnect.model.CapacityDescription;
import software.amazon.awssdk.services.kafkaconnect.model.CapacityUpdate;
import software.amazon.awssdk.services.kafkaconnect.model.CloudWatchLogsLogDeliveryDescription;
import software.amazon.awssdk.services.kafkaconnect.model.ConnectorState;
import software.amazon.awssdk.services.kafkaconnect.model.ConnectorSummary;
import software.amazon.awssdk.services.kafkaconnect.model.CreateConnectorRequest;
import software.amazon.awssdk.services.kafkaconnect.model.CustomPluginDescription;
import software.amazon.awssdk.services.kafkaconnect.model.DeleteConnectorRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeConnectorRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeConnectorResponse;
import software.amazon.awssdk.services.kafkaconnect.model.FirehoseLogDeliveryDescription;
import software.amazon.awssdk.services.kafkaconnect.model.KafkaClusterClientAuthenticationDescription;
import software.amazon.awssdk.services.kafkaconnect.model.KafkaClusterClientAuthenticationType;
import software.amazon.awssdk.services.kafkaconnect.model.KafkaClusterDescription;
import software.amazon.awssdk.services.kafkaconnect.model.KafkaClusterEncryptionInTransitDescription;
import software.amazon.awssdk.services.kafkaconnect.model.KafkaClusterEncryptionInTransitType;
import software.amazon.awssdk.services.kafkaconnect.model.ListConnectorsRequest;
import software.amazon.awssdk.services.kafkaconnect.model.ListConnectorsResponse;
import software.amazon.awssdk.services.kafkaconnect.model.LogDeliveryDescription;
import software.amazon.awssdk.services.kafkaconnect.model.PluginDescription;
import software.amazon.awssdk.services.kafkaconnect.model.ProvisionedCapacityDescription;
import software.amazon.awssdk.services.kafkaconnect.model.ProvisionedCapacityUpdate;
import software.amazon.awssdk.services.kafkaconnect.model.S3LogDeliveryDescription;
import software.amazon.awssdk.services.kafkaconnect.model.ScaleInPolicyDescription;
import software.amazon.awssdk.services.kafkaconnect.model.ScaleInPolicyUpdate;
import software.amazon.awssdk.services.kafkaconnect.model.ScaleOutPolicyDescription;
import software.amazon.awssdk.services.kafkaconnect.model.ScaleOutPolicyUpdate;
import software.amazon.awssdk.services.kafkaconnect.model.UpdateConnectorRequest;
import software.amazon.awssdk.services.kafkaconnect.model.VpcDescription;
import software.amazon.awssdk.services.kafkaconnect.model.WorkerConfigurationDescription;
import software.amazon.awssdk.services.kafkaconnect.model.WorkerLogDeliveryDescription;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
public class TranslatorTest {

    private Translator translator = new Translator();

    @Test
    public void translateToCreateRequest_fullConnector_success() {
        compareCreateRequest(translator.translateToCreateRequest(TestData.FULL_RESOURCE_CREATE_MODEL),
            TestData.COMPLETE_CREATE_CONNECTOR_REQUEST);
    }

    @Test
    public void translateToCreateRequest_minimalConnector_success() {
        compareCreateRequest(translator.translateToCreateRequest(TestData.MINIMAL_RESOURCE_CREATE_MODEL),
            TestData.MINIMAL_CREATE_CONNECTOR_REQUEST);
    }

    @Test
    public void translateToReadRequest_success() {
        assertThat(translator.translateToReadRequest(TestData.READ_REQUEST_RESOURCE_MODEL))
            .isEqualTo(TestData.DESCRIBE_CONNECTOR_REQUEST);
    }

    @Test
    public void translateFromReadResponse_fullConnector_success() {
        assertThat(translator.translateFromReadResponse(TestData.FULL_DESCRIBE_CONNECTOR_RESPONSE))
            .isEqualTo(TestData.FULL_RESOURCE_DESCRIBE_MODEL);
    }

    @Test
    public void translateFromReadResponse_minimalConnector_success() {
        assertThat(translator.translateFromReadResponse(TestData.MINIMAL_DESCRIBE_CONNECTOR_RESPONSE))
            .isEqualTo(TestData.MINIMAL_RESOURCE_DESCRIBE_MODEL);
    }

    @Test
    public void translateToDeleteRequest_success() {
        assertThat(translator.translateToDeleteRequest(TestData.DELETE_REQUEST_RESOURCE_MODEL))
            .isEqualTo(TestData.DELETE_CONNECTOR_REQUEST);
    }

    @Test
    public void translateToListRequest_success() {
        assertThat(translator.translateToListRequest(TestData.NEXT_TOKEN))
            .isEqualTo(TestData.LIST_CONNECTORS_REQUEST);
    }

    @Test
    public void translateFromListResponse_success() {
        assertThat(translator.translateFromListResponse(TestData.LIST_CONNECTORS_RESPONSE))
            .isEqualTo(TestData.LIST_CONNECTORS_MODELS);
    }

    @Test
    public void translateToUpdateRequest_success() {
        assertThat(translator.translateToUpdateRequest(TestData.UPDATE_REQUEST_RESOURCE_MODEL))
            .isEqualTo(TestData.UPDATE_CONNECTOR_REQUEST);
    }

    private void compareCreateRequest(final CreateConnectorRequest request1, final CreateConnectorRequest request2) {
        // compare all fields without arrays
        assertThat(request1.capacity()).isEqualTo(request2.capacity());
        assertThat(request1.connectorConfiguration()).isEqualTo(request2.connectorConfiguration());
        assertThat(request1.connectorDescription()).isEqualTo(request2.connectorDescription());
        assertThat(request1.connectorName()).isEqualTo(request2.connectorName());
        assertThat(request1.kafkaClusterClientAuthentication()).isEqualTo(request2.kafkaClusterClientAuthentication());
        assertThat(request1.kafkaClusterEncryptionInTransit()).isEqualTo(request2.kafkaClusterEncryptionInTransit());
        assertThat(request1.kafkaConnectVersion()).isEqualTo(request2.kafkaConnectVersion());
        assertThat(request1.logDelivery()).isEqualTo(request2.logDelivery());
        assertThat(request1.serviceExecutionRoleArn()).isEqualTo(request2.serviceExecutionRoleArn());
        assertThat(request1.workerConfiguration()).isEqualTo(request2.workerConfiguration());

        compareKafkaClusters(request1.kafkaCluster().apacheKafkaCluster(), request2.kafkaCluster().apacheKafkaCluster());
        comparePlugins(request1.plugins(), request2.plugins());
    }

    private void compareKafkaClusters(
        final software.amazon.awssdk.services.kafkaconnect.model.ApacheKafkaCluster apacheCluster1,
        final software.amazon.awssdk.services.kafkaconnect.model.ApacheKafkaCluster apacheCluster2) {

        assertThat(apacheCluster1.bootstrapServers()).isEqualTo(apacheCluster2.bootstrapServers());
        assertTrue(areListsEqual(apacheCluster1.vpc().subnets(), apacheCluster2.vpc().subnets()));
        assertTrue(areListsEqual(apacheCluster1.vpc().securityGroups(), apacheCluster2.vpc().securityGroups()));
    }

    private void comparePlugins(final List<software.amazon.awssdk.services.kafkaconnect.model.Plugin> plugins1,
        final List<software.amazon.awssdk.services.kafkaconnect.model.Plugin> plugins2) {

        assertTrue(areListsEqual(plugins1, plugins2));
    }

    private <T> boolean areListsEqual(final List<T> list1, final List<T> list2) {
        final Set<T> set1 = new HashSet<T>(list1);
        final Set<T> set2 = new HashSet<T>(list2);
        return set1.equals(set2);
    }

    private static class TestData {
        private static final String CONNECTOR_NAME = "unit-test-connector";
        private static final String CONNECTOR_NAME_2 = "unit-test-connector-2";
        private static final String CONNECTOR_DESCRIPTION = "Unit testing connector description";
        private static final String CONNECTOR_ARN =
            "arn:aws:kafkaconnect:us-east-1:123456789:connector/unit-test-connector";
        private static final String CONNECTOR_ARN_2 =
            "arn:aws:kafkaconnect:us-east-1:123456789:connector2/unit-test-connector";
        private static final Map<String, String> CONNECTOR_CONFIGURATION = new HashMap<String, String>() {{
            put("tasks.max", "2");
            put("connector.class", "io.confluent.connect.s3.S3SinkConnector");
        }};
        private static final ConnectorState CONNECTOR_STATE = ConnectorState.RUNNING;
        private static final String KAFKA_CONNECT_VERSION = "2.7.1";
        private static final String CLOUDWATCH_LOG_GROUP = "unit-test-cloud-watch-log-group";
        private static final boolean CLOUDWATCH_ENABLED = true;
        private static final String FIREHOSE_DELIVERY_STREAM = "unit-test-fire-hose-delivery-stream";
        private static final boolean FIREHOSE_ENABLED = true;
        private static final String S3_BUCKET = "unit-test-s3-bucket";
        private static final String S3_PREFIX = "unit-test-s3-prefix";
        private static final boolean S3_ENABLED = true;
        private static final String SERVICE_EXECUTION_ROLE_ARN =
            "arn:aws:iam:us-east-1:123456789:iam-role/unit-test-service-execution-role";
        private static final String CUSTOM_PLUGIN_ARN =
            "arn:aws:kafkaconnect:us-east-1:123456789:custom-plugin/unit-test-custom-plugin";
        private static final long CUSTOM_PLUGIN_REVISION = 1L;
        private static final int WORKER_COUNT = 1;
        private static final int MCU_COUNT = 2;
        private static final String KAFKA_CLUSTER_BOOTSTRAP_SERVERS = "z-3.unit-test-cluster.12345.q10." +
            "kafka.us-east-1.amazonaws.com:1234,z-3.unit-test-cluster.12345.q10.kafka.us-east-1.amazonaws.com:1234";
        private static final String VPC_SUBNET_1 = "subnet-12345";
        private static final String VPC_SUBNET_2 = "subnet-67890";
        private static final String VPC_SECURITY_GROUP = "sg-12345";
        private static final String WORKER_CONFIGURATION_ARN =
            "arn:aws:kafkaconnect:us-east-1:123456789:worker-configuration/unit-test-worker-configuration";
        private static final long WORKER_CONFIGURATION_REVISION = 1L;
        private static final Instant CONNECTOR_CREATION_TIME =
            OffsetDateTime.parse("2021-03-04T14:03:40.818Z").toInstant();
        private static final String CURRENT_VERSION = "AB1CDQEFGHZ5";
        private static final String NEXT_TOKEN = "1234abcd";
        private static final int MAX_WORKER_COUNT = 10;
        private static final int MIN_WORKER_COUNT = 1;
        private static final int SCALE_IN_UTIL_PERCENT = 75;
        private static final int SCALE_OUT_UTIL_PERCENT = 85;
        private static final String AUTHENTICATION_TYPE =
            KafkaClusterClientAuthenticationType.NONE.toString();
        private static final String ENCRYPTION_IN_TRANSIT_TYPE =
            KafkaClusterEncryptionInTransitType.PLAINTEXT.toString();

        private static final ResourceModel FULL_RESOURCE_CREATE_MODEL = ResourceModel
            .builder()
            .capacity(Capacity.builder()
                .provisionedCapacity(ProvisionedCapacity.builder()
                    .workerCount(WORKER_COUNT)
                    .mcuCount(MCU_COUNT)
                    .build())
                .autoScaling(AutoScaling.builder()
                    .maxWorkerCount(MAX_WORKER_COUNT)
                    .minWorkerCount(MIN_WORKER_COUNT)
                    .scaleInPolicy(ScaleInPolicy.builder()
                        .cpuUtilizationPercentage(SCALE_IN_UTIL_PERCENT)
                        .build())
                    .scaleOutPolicy(ScaleOutPolicy.builder()
                        .cpuUtilizationPercentage(SCALE_OUT_UTIL_PERCENT)
                        .build())
                    .mcuCount(MCU_COUNT)
                    .build())
                .build())
            .connectorConfiguration(CONNECTOR_CONFIGURATION)
            .connectorDescription(CONNECTOR_DESCRIPTION)
            .connectorName(CONNECTOR_NAME)
            .kafkaCluster(KafkaCluster.builder()
                .apacheKafkaCluster(ApacheKafkaCluster.builder()
                    .bootstrapServers(KAFKA_CLUSTER_BOOTSTRAP_SERVERS)
                    .vpc(Vpc.builder()
                        .subnets(new HashSet<>(asList(VPC_SUBNET_1, VPC_SUBNET_2)))
                        .securityGroups(new HashSet<>(asList(VPC_SECURITY_GROUP)))
                        .build())
                    .build())
                .build())
            .kafkaClusterClientAuthentication(KafkaClusterClientAuthentication.builder()
                .authenticationType(AUTHENTICATION_TYPE)
                .build())
            .kafkaClusterEncryptionInTransit(KafkaClusterEncryptionInTransit.builder()
                .encryptionType(ENCRYPTION_IN_TRANSIT_TYPE)
                .build())
            .kafkaConnectVersion(KAFKA_CONNECT_VERSION)
            .logDelivery(LogDelivery.builder()
                .workerLogDelivery(WorkerLogDelivery.builder()
                    .cloudWatchLogs(CloudWatchLogsLogDelivery.builder()
                        .logGroup(CLOUDWATCH_LOG_GROUP)
                        .enabled(CLOUDWATCH_ENABLED)
                        .build())
                    .firehose(FirehoseLogDelivery.builder()
                        .deliveryStream(FIREHOSE_DELIVERY_STREAM)
                        .enabled(FIREHOSE_ENABLED)
                        .build())
                    .s3(S3LogDelivery.builder()
                        .bucket(S3_BUCKET)
                        .prefix(S3_PREFIX)
                        .enabled(S3_ENABLED)
                        .build())
                    .build())
                .build())
            .plugins(new HashSet<>(asList(Plugin.builder()
                .customPlugin(CustomPlugin.builder()
                    .customPluginArn(CUSTOM_PLUGIN_ARN)
                    .revision(CUSTOM_PLUGIN_REVISION)
                    .build())
                .build())))
            .serviceExecutionRoleArn(SERVICE_EXECUTION_ROLE_ARN)
            .workerConfiguration(WorkerConfiguration.builder()
                .workerConfigurationArn(WORKER_CONFIGURATION_ARN)
                .revision(WORKER_CONFIGURATION_REVISION)
                .build())
            .build();

        private static final CreateConnectorRequest COMPLETE_CREATE_CONNECTOR_REQUEST =
            CreateConnectorRequest.builder()
                .capacity(software.amazon.awssdk.services.kafkaconnect.model.Capacity.builder()
                    .autoScaling(software.amazon.awssdk.services.kafkaconnect.model.AutoScaling.builder()
                        .maxWorkerCount(MAX_WORKER_COUNT)
                        .minWorkerCount(MIN_WORKER_COUNT)
                        .scaleInPolicy(software.amazon.awssdk.services.kafkaconnect.model.ScaleInPolicy.builder()
                            .cpuUtilizationPercentage(SCALE_IN_UTIL_PERCENT)
                            .build())
                        .scaleOutPolicy(software.amazon.awssdk.services.kafkaconnect.model.ScaleOutPolicy.builder()
                            .cpuUtilizationPercentage(SCALE_OUT_UTIL_PERCENT)
                            .build())
                        .mcuCount(MCU_COUNT)
                        .build())
                    .provisionedCapacity(software.amazon.awssdk.services.kafkaconnect.model.ProvisionedCapacity
                        .builder()
                        .workerCount(WORKER_COUNT)
                        .mcuCount(MCU_COUNT)
                        .build())
                    .build())
                .connectorConfiguration(CONNECTOR_CONFIGURATION)
                .connectorDescription(CONNECTOR_DESCRIPTION)
                .connectorName(CONNECTOR_NAME)
                .kafkaCluster(software.amazon.awssdk.services.kafkaconnect.model.KafkaCluster.builder()
                    .apacheKafkaCluster(software.amazon.awssdk.services.kafkaconnect.model.ApacheKafkaCluster
                        .builder()
                        .bootstrapServers(KAFKA_CLUSTER_BOOTSTRAP_SERVERS)
                        .vpc(software.amazon.awssdk.services.kafkaconnect.model.Vpc.builder()
                            .subnets(asList(VPC_SUBNET_1, VPC_SUBNET_2))
                            .securityGroups(asList(VPC_SECURITY_GROUP))
                            .build())
                        .build())
                    .build())
                .kafkaClusterClientAuthentication(
                    software.amazon.awssdk.services.kafkaconnect.model.KafkaClusterClientAuthentication.builder()
                    .authenticationType(AUTHENTICATION_TYPE)
                    .build())
                .kafkaClusterEncryptionInTransit(
                    software.amazon.awssdk.services.kafkaconnect.model.KafkaClusterEncryptionInTransit.builder()
                    .encryptionType(ENCRYPTION_IN_TRANSIT_TYPE)
                    .build())
                .kafkaConnectVersion(KAFKA_CONNECT_VERSION)
                .logDelivery(software.amazon.awssdk.services
                    .kafkaconnect.model.LogDelivery.builder()
                    .workerLogDelivery(software.amazon.awssdk.services
                        .kafkaconnect.model.WorkerLogDelivery.builder()
                        .cloudWatchLogs(software.amazon.awssdk.services
                            .kafkaconnect.model.CloudWatchLogsLogDelivery.builder()
                            .logGroup(CLOUDWATCH_LOG_GROUP)
                            .enabled(CLOUDWATCH_ENABLED)
                            .build())
                        .firehose(software.amazon.awssdk.services
                            .kafkaconnect.model.FirehoseLogDelivery.builder()
                            .deliveryStream(FIREHOSE_DELIVERY_STREAM)
                            .enabled(FIREHOSE_ENABLED)
                            .build())
                        .s3(software.amazon.awssdk.services
                            .kafkaconnect.model.S3LogDelivery.builder()
                            .bucket(S3_BUCKET)
                            .prefix(S3_PREFIX)
                            .enabled(S3_ENABLED)
                            .build())
                        .build())
                    .build())
                .plugins(asList(software.amazon.awssdk.services
                    .kafkaconnect.model.Plugin.builder()
                    .customPlugin(software.amazon.awssdk.services
                        .kafkaconnect.model.CustomPlugin.builder()
                        .customPluginArn(CUSTOM_PLUGIN_ARN)
                        .revision(CUSTOM_PLUGIN_REVISION)
                        .build())
                    .build()))
                .serviceExecutionRoleArn(SERVICE_EXECUTION_ROLE_ARN)
                .workerConfiguration(software.amazon.awssdk.services
                    .kafkaconnect.model.WorkerConfiguration.builder()
                    .workerConfigurationArn(WORKER_CONFIGURATION_ARN)
                    .revision(WORKER_CONFIGURATION_REVISION)
                    .build())
                .build();

        private static final ResourceModel MINIMAL_RESOURCE_CREATE_MODEL =
            ResourceModel.builder()
                .capacity(Capacity.builder()
                    .provisionedCapacity(ProvisionedCapacity.builder()
                        .workerCount(WORKER_COUNT)
                        .mcuCount(MCU_COUNT)
                        .build())
                    .build())
                .connectorConfiguration(CONNECTOR_CONFIGURATION)
                .connectorName(CONNECTOR_NAME)
                .kafkaCluster(KafkaCluster.builder()
                    .apacheKafkaCluster(ApacheKafkaCluster.builder()
                        .bootstrapServers(KAFKA_CLUSTER_BOOTSTRAP_SERVERS)
                        .vpc(Vpc.builder()
                            .subnets(new HashSet<>(asList(VPC_SUBNET_1)))
                            .build())
                        .build())
                    .build())
                .kafkaClusterClientAuthentication(KafkaClusterClientAuthentication.builder()
                    .authenticationType(AUTHENTICATION_TYPE)
                    .build())
                .kafkaClusterEncryptionInTransit(KafkaClusterEncryptionInTransit.builder()
                    .encryptionType(ENCRYPTION_IN_TRANSIT_TYPE)
                    .build())
                .kafkaConnectVersion(KAFKA_CONNECT_VERSION)
                .plugins(new HashSet<>(asList(Plugin.builder()
                    .customPlugin(CustomPlugin.builder()
                        .customPluginArn(CUSTOM_PLUGIN_ARN)
                        .revision(CUSTOM_PLUGIN_REVISION)
                        .build())
                    .build())))
                .serviceExecutionRoleArn(SERVICE_EXECUTION_ROLE_ARN)
                .build();

        private static final CreateConnectorRequest MINIMAL_CREATE_CONNECTOR_REQUEST =
            CreateConnectorRequest.builder()
                .capacity(software.amazon.awssdk.services.kafkaconnect.model.Capacity.builder()
                    .provisionedCapacity(software.amazon.awssdk.services.kafkaconnect.model.ProvisionedCapacity
                        .builder()
                        .workerCount(WORKER_COUNT)
                        .mcuCount(MCU_COUNT)
                        .build())
                    .build())
                .connectorConfiguration(CONNECTOR_CONFIGURATION)
                .connectorName(CONNECTOR_NAME)
                .kafkaCluster(software.amazon.awssdk.services.kafkaconnect.model.KafkaCluster.builder()
                    .apacheKafkaCluster(software.amazon.awssdk.services.kafkaconnect.model.ApacheKafkaCluster
                        .builder()
                        .bootstrapServers(KAFKA_CLUSTER_BOOTSTRAP_SERVERS)
                        .vpc(software.amazon.awssdk.services.kafkaconnect.model.Vpc.builder()
                            .subnets(asList(VPC_SUBNET_1))
                            .build())
                        .build())
                    .build())
                .kafkaClusterClientAuthentication(
                    software.amazon.awssdk.services.kafkaconnect.model.KafkaClusterClientAuthentication.builder()
                        .authenticationType(AUTHENTICATION_TYPE)
                        .build())
                .kafkaClusterEncryptionInTransit(
                    software.amazon.awssdk.services.kafkaconnect.model.KafkaClusterEncryptionInTransit.builder()
                        .encryptionType(ENCRYPTION_IN_TRANSIT_TYPE)
                        .build())
                .kafkaConnectVersion(KAFKA_CONNECT_VERSION)
                .plugins(asList(software.amazon.awssdk.services
                    .kafkaconnect.model.Plugin.builder()
                    .customPlugin(software.amazon.awssdk.services
                        .kafkaconnect.model.CustomPlugin.builder()
                        .customPluginArn(CUSTOM_PLUGIN_ARN)
                        .revision(CUSTOM_PLUGIN_REVISION)
                        .build())
                    .build()))
                .serviceExecutionRoleArn(SERVICE_EXECUTION_ROLE_ARN)
                .build();

        private static final ResourceModel READ_REQUEST_RESOURCE_MODEL =
            ResourceModel.builder()
                .connectorArn(CONNECTOR_ARN)
                .build();

        private static final ResourceModel DELETE_REQUEST_RESOURCE_MODEL =
            ResourceModel.builder()
                .connectorArn(CONNECTOR_ARN)
                .build();

        private static final ResourceModel UPDATE_REQUEST_RESOURCE_MODEL =
            ResourceModel.builder()
                .capacity(Capacity.builder()
                    .provisionedCapacity(ProvisionedCapacity.builder()
                        .workerCount(WORKER_COUNT)
                        .mcuCount(MCU_COUNT)
                        .build())
                    .autoScaling(AutoScaling.builder()
                        .maxWorkerCount(MAX_WORKER_COUNT)
                        .minWorkerCount(MIN_WORKER_COUNT)
                        .scaleInPolicy(ScaleInPolicy.builder()
                            .cpuUtilizationPercentage(SCALE_IN_UTIL_PERCENT)
                            .build())
                        .scaleOutPolicy(ScaleOutPolicy.builder()
                            .cpuUtilizationPercentage(SCALE_OUT_UTIL_PERCENT)
                            .build())
                        .mcuCount(MCU_COUNT)
                        .build())
                    .build())
                .connectorArn(CONNECTOR_ARN)
                .build();

        private static final DescribeConnectorRequest DESCRIBE_CONNECTOR_REQUEST =
            DescribeConnectorRequest.builder()
                .connectorArn(CONNECTOR_ARN)
                .build();

        private static final UpdateConnectorRequest UPDATE_CONNECTOR_REQUEST =
            UpdateConnectorRequest.builder()
                .connectorArn(CONNECTOR_ARN)
                .capacity(CapacityUpdate.builder()
                    .provisionedCapacity(ProvisionedCapacityUpdate.builder()
                        .workerCount(WORKER_COUNT)
                        .mcuCount(MCU_COUNT)
                        .build())
                    .autoScaling(AutoScalingUpdate.builder()
                        .maxWorkerCount(MAX_WORKER_COUNT)
                        .minWorkerCount(MIN_WORKER_COUNT)
                        .scaleInPolicy(ScaleInPolicyUpdate.builder()
                            .cpuUtilizationPercentage(SCALE_IN_UTIL_PERCENT)
                            .build())
                        .scaleOutPolicy(ScaleOutPolicyUpdate.builder()
                            .cpuUtilizationPercentage(SCALE_OUT_UTIL_PERCENT)
                            .build())
                        .mcuCount(MCU_COUNT)
                        .build())
                    .build())
                .build();

        private static final DescribeConnectorResponse MINIMAL_DESCRIBE_CONNECTOR_RESPONSE =
            DescribeConnectorResponse.builder()
                .capacity(CapacityDescription.builder()
                    .provisionedCapacity(ProvisionedCapacityDescription.builder()
                        .workerCount(WORKER_COUNT)
                        .mcuCount(MCU_COUNT)
                        .build())
                    .build())
                .connectorArn(CONNECTOR_ARN)
                .connectorConfiguration(CONNECTOR_CONFIGURATION)
                .connectorName(CONNECTOR_NAME)
                .connectorState(ConnectorState.RUNNING)
                .creationTime(CONNECTOR_CREATION_TIME)
                .currentVersion(CURRENT_VERSION)
                .kafkaCluster(KafkaClusterDescription.builder()
                    .apacheKafkaCluster(ApacheKafkaClusterDescription.builder()
                        .bootstrapServers(KAFKA_CLUSTER_BOOTSTRAP_SERVERS)
                        .vpc(VpcDescription.builder()
                            .subnets(asList(VPC_SUBNET_1))
                            .securityGroups(asList(VPC_SECURITY_GROUP))
                            .build())
                        .build())
                    .build())
                .kafkaClusterClientAuthentication(KafkaClusterClientAuthenticationDescription.builder()
                    .authenticationType(AUTHENTICATION_TYPE)
                    .build())
                .kafkaClusterEncryptionInTransit(KafkaClusterEncryptionInTransitDescription.builder()
                    .encryptionType(ENCRYPTION_IN_TRANSIT_TYPE)
                    .build())
                .kafkaConnectVersion(KAFKA_CONNECT_VERSION)
                .plugins(asList(PluginDescription.builder()
                    .customPlugin(CustomPluginDescription.builder()
                        .customPluginArn(CUSTOM_PLUGIN_ARN)
                        .revision(CUSTOM_PLUGIN_REVISION)
                        .build())
                    .build()))
                .serviceExecutionRoleArn(SERVICE_EXECUTION_ROLE_ARN)
                .connectorState(CONNECTOR_STATE)
                .build();

        private static DescribeConnectorResponse FULL_DESCRIBE_CONNECTOR_RESPONSE =
            DescribeConnectorResponse
                .builder()
                .capacity(capacityDescription())
                .connectorArn(CONNECTOR_ARN)
                .connectorConfiguration(CONNECTOR_CONFIGURATION)
                .connectorDescription(CONNECTOR_DESCRIPTION)
                .connectorName(CONNECTOR_NAME)
                .connectorState(CONNECTOR_STATE)
                .creationTime(CONNECTOR_CREATION_TIME)
                .currentVersion(CURRENT_VERSION)
                .kafkaCluster(kafkaClusterDescription())
                .kafkaClusterClientAuthentication(kafkaClusterClientAuthenticationDescription())
                .kafkaClusterEncryptionInTransit(kafkaClusterEncryptionInTransitDescription())
                .kafkaConnectVersion(KAFKA_CONNECT_VERSION)
                .logDelivery(logDeliveryDescription())
                .plugins(asList(pluginDescription()))
                .serviceExecutionRoleArn(SERVICE_EXECUTION_ROLE_ARN)
                .workerConfiguration(workerConfigurationDescription())
                .build();

        private static final ResourceModel MINIMAL_RESOURCE_DESCRIBE_MODEL =
            ResourceModel.builder()
                .capacity(Capacity.builder()
                    .provisionedCapacity(ProvisionedCapacity.builder()
                        .workerCount(WORKER_COUNT)
                        .mcuCount(MCU_COUNT)
                        .build())
                    .build())
                .connectorArn(CONNECTOR_ARN)
                .connectorConfiguration(CONNECTOR_CONFIGURATION)
                .connectorName(CONNECTOR_NAME)
                .kafkaCluster(KafkaCluster.builder()
                    .apacheKafkaCluster(ApacheKafkaCluster.builder()
                        .bootstrapServers(KAFKA_CLUSTER_BOOTSTRAP_SERVERS)
                        .vpc(Vpc.builder()
                            .subnets(new HashSet<>(asList(VPC_SUBNET_1)))
                            .securityGroups(new HashSet<>(asList(VPC_SECURITY_GROUP)))
                            .build())
                        .build())
                    .build())
                .kafkaClusterClientAuthentication(KafkaClusterClientAuthentication.builder()
                    .authenticationType(AUTHENTICATION_TYPE)
                    .build())
                .kafkaClusterEncryptionInTransit(KafkaClusterEncryptionInTransit.builder()
                    .encryptionType(ENCRYPTION_IN_TRANSIT_TYPE)
                    .build())
                .kafkaConnectVersion(KAFKA_CONNECT_VERSION)
                .plugins(new HashSet<>(asList(Plugin.builder()
                    .customPlugin(CustomPlugin.builder()
                        .customPluginArn(CUSTOM_PLUGIN_ARN)
                        .revision(CUSTOM_PLUGIN_REVISION)
                        .build())
                    .build())))
                .serviceExecutionRoleArn(SERVICE_EXECUTION_ROLE_ARN)
                .build();

        private static final ResourceModel FULL_RESOURCE_DESCRIBE_MODEL = fullModel(CONNECTOR_NAME, CONNECTOR_ARN);

        private static final DeleteConnectorRequest DELETE_CONNECTOR_REQUEST =
            DeleteConnectorRequest.builder()
                .connectorArn(CONNECTOR_ARN)
                .build();

        private static final ListConnectorsRequest LIST_CONNECTORS_REQUEST =
            ListConnectorsRequest.builder()
                .nextToken(NEXT_TOKEN)
                .build();

        private static final List<ResourceModel> LIST_CONNECTORS_MODELS = asList(
            modelWithoutConnectorConfiguration(CONNECTOR_NAME, CONNECTOR_ARN),
            modelWithoutConnectorConfiguration(CONNECTOR_NAME_2, CONNECTOR_ARN_2));

        private static final ListConnectorsResponse LIST_CONNECTORS_RESPONSE =
            ListConnectorsResponse.builder()
                .connectors(asList(fullConnectorSummary(CONNECTOR_NAME, CONNECTOR_ARN),
                    fullConnectorSummary(CONNECTOR_NAME_2, CONNECTOR_ARN_2)))
                .build();

        private static ResourceModel modelWithoutConnectorConfiguration(final String name, final String arn) {
            return ResourceModel
                .builder()
                .capacity(Capacity.builder()
                    .provisionedCapacity(ProvisionedCapacity.builder()
                        .workerCount(WORKER_COUNT)
                        .mcuCount(MCU_COUNT)
                        .build())
                    .autoScaling(AutoScaling.builder()
                        .maxWorkerCount(MAX_WORKER_COUNT)
                        .minWorkerCount(MIN_WORKER_COUNT)
                        .scaleInPolicy(ScaleInPolicy.builder()
                            .cpuUtilizationPercentage(SCALE_IN_UTIL_PERCENT)
                            .build())
                        .scaleOutPolicy(ScaleOutPolicy.builder()
                            .cpuUtilizationPercentage(SCALE_OUT_UTIL_PERCENT)
                            .build())
                        .mcuCount(MCU_COUNT)
                        .build())
                    .build())
                .connectorDescription(CONNECTOR_DESCRIPTION)
                .connectorName(name)
                .connectorArn(arn)
                .kafkaCluster(KafkaCluster.builder()
                    .apacheKafkaCluster(ApacheKafkaCluster.builder()
                        .bootstrapServers(KAFKA_CLUSTER_BOOTSTRAP_SERVERS)
                        .vpc(Vpc.builder()
                            .subnets(new HashSet<>(asList(VPC_SUBNET_1, VPC_SUBNET_2)))
                            .securityGroups(new HashSet<>(asList(VPC_SECURITY_GROUP)))
                            .build())
                        .build())
                    .build())
                .kafkaClusterClientAuthentication(KafkaClusterClientAuthentication.builder()
                    .authenticationType(AUTHENTICATION_TYPE)
                    .build())
                .kafkaClusterEncryptionInTransit(KafkaClusterEncryptionInTransit.builder()
                    .encryptionType(ENCRYPTION_IN_TRANSIT_TYPE)
                    .build())
                .kafkaConnectVersion(KAFKA_CONNECT_VERSION)
                .logDelivery(LogDelivery.builder()
                    .workerLogDelivery(WorkerLogDelivery.builder()
                        .cloudWatchLogs(CloudWatchLogsLogDelivery.builder()
                            .logGroup(CLOUDWATCH_LOG_GROUP)
                            .enabled(CLOUDWATCH_ENABLED)
                            .build())
                        .firehose(FirehoseLogDelivery.builder()
                            .deliveryStream(FIREHOSE_DELIVERY_STREAM)
                            .enabled(FIREHOSE_ENABLED)
                            .build())
                        .s3(S3LogDelivery.builder()
                            .bucket(S3_BUCKET)
                            .prefix(S3_PREFIX)
                            .enabled(S3_ENABLED)
                            .build())
                        .build())
                    .build())
                .plugins(new HashSet<>(asList(Plugin.builder()
                    .customPlugin(CustomPlugin.builder()
                        .customPluginArn(CUSTOM_PLUGIN_ARN)
                        .revision(CUSTOM_PLUGIN_REVISION)
                        .build())
                    .build())))
                .serviceExecutionRoleArn(SERVICE_EXECUTION_ROLE_ARN)
                .workerConfiguration(WorkerConfiguration.builder()
                    .workerConfigurationArn(WORKER_CONFIGURATION_ARN)
                    .revision(WORKER_CONFIGURATION_REVISION)
                    .build())
                .build();
        }

        private static ResourceModel fullModel(final String name, final String arn) {
            final ResourceModel resourceModel = modelWithoutConnectorConfiguration(name, arn);
            resourceModel.setConnectorConfiguration(CONNECTOR_CONFIGURATION);
            return resourceModel;
        }

        private static ConnectorSummary fullConnectorSummary(final String name, final String arn) {
            return ConnectorSummary.builder()
                .capacity(capacityDescription())
                .connectorArn(arn)
                .connectorDescription(CONNECTOR_DESCRIPTION)
                .connectorName(name)
                .connectorState(CONNECTOR_STATE)
                .creationTime(CONNECTOR_CREATION_TIME)
                .currentVersion(CURRENT_VERSION)
                .kafkaCluster(kafkaClusterDescription())
                .kafkaClusterClientAuthentication(kafkaClusterClientAuthenticationDescription())
                .kafkaClusterEncryptionInTransit(kafkaClusterEncryptionInTransitDescription())
                .kafkaConnectVersion(KAFKA_CONNECT_VERSION)
                .logDelivery(logDeliveryDescription())
                .plugins(asList(pluginDescription()))
                .serviceExecutionRoleArn(SERVICE_EXECUTION_ROLE_ARN)
                .workerConfiguration(workerConfigurationDescription())
                .build();
        }

        private static CapacityDescription capacityDescription() {
            return CapacityDescription.builder()
                .autoScaling(AutoScalingDescription.builder()
                    .maxWorkerCount(MAX_WORKER_COUNT)
                    .minWorkerCount(MIN_WORKER_COUNT)
                    .scaleInPolicy(ScaleInPolicyDescription.builder()
                        .cpuUtilizationPercentage(SCALE_IN_UTIL_PERCENT)
                        .build())
                    .scaleOutPolicy(ScaleOutPolicyDescription.builder()
                        .cpuUtilizationPercentage(SCALE_OUT_UTIL_PERCENT)
                        .build())
                    .mcuCount(MCU_COUNT)
                    .build())
                .provisionedCapacity(ProvisionedCapacityDescription.builder()
                    .workerCount(WORKER_COUNT)
                    .mcuCount(MCU_COUNT)
                    .build())
                .build();
        }

        private static KafkaClusterDescription kafkaClusterDescription() {
            return KafkaClusterDescription.builder()
                .apacheKafkaCluster(ApacheKafkaClusterDescription.builder()
                    .bootstrapServers(KAFKA_CLUSTER_BOOTSTRAP_SERVERS)
                    .vpc(VpcDescription.builder()
                        .subnets(asList(VPC_SUBNET_1, VPC_SUBNET_2))
                        .securityGroups(asList(VPC_SECURITY_GROUP))
                        .build())
                    .build())
                .build();
        }

        private static KafkaClusterClientAuthenticationDescription kafkaClusterClientAuthenticationDescription() {
            return KafkaClusterClientAuthenticationDescription.builder()
                .authenticationType(AUTHENTICATION_TYPE)
                .build();
        }

        private static KafkaClusterEncryptionInTransitDescription kafkaClusterEncryptionInTransitDescription() {
            return KafkaClusterEncryptionInTransitDescription.builder()
                .encryptionType(ENCRYPTION_IN_TRANSIT_TYPE)
                .build();
        }

        private static LogDeliveryDescription logDeliveryDescription() {
            return LogDeliveryDescription.builder()
                .workerLogDelivery(WorkerLogDeliveryDescription.builder()
                    .cloudWatchLogs(CloudWatchLogsLogDeliveryDescription.builder()
                        .logGroup(CLOUDWATCH_LOG_GROUP)
                        .enabled(CLOUDWATCH_ENABLED)
                        .build())
                    .firehose(FirehoseLogDeliveryDescription.builder()
                        .deliveryStream(FIREHOSE_DELIVERY_STREAM)
                        .enabled(FIREHOSE_ENABLED)
                        .build())
                    .s3(S3LogDeliveryDescription.builder()
                        .bucket(S3_BUCKET)
                        .prefix(S3_PREFIX)
                        .enabled(S3_ENABLED)
                        .build())
                    .build())
                .build();
        }

        private static PluginDescription pluginDescription() {
            return PluginDescription.builder()
                .customPlugin(CustomPluginDescription.builder()
                    .customPluginArn(CUSTOM_PLUGIN_ARN)
                    .revision(CUSTOM_PLUGIN_REVISION)
                    .build())
                .build();
        }

        private static WorkerConfigurationDescription workerConfigurationDescription() {
            return WorkerConfigurationDescription.builder()
                .revision(WORKER_CONFIGURATION_REVISION)
                .workerConfigurationArn(WORKER_CONFIGURATION_ARN)
                .build();
        }
    }
}
