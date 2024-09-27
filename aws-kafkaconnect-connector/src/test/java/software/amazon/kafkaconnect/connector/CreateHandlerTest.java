package software.amazon.kafkaconnect.connector;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.kafkaconnect.KafkaConnectClient;
import software.amazon.awssdk.services.kafkaconnect.model.ApacheKafkaCluster;
import software.amazon.awssdk.services.kafkaconnect.model.Capacity;
import software.amazon.awssdk.services.kafkaconnect.model.ConflictException;
import software.amazon.awssdk.services.kafkaconnect.model.ConnectorState;
import software.amazon.awssdk.services.kafkaconnect.model.CreateConnectorRequest;
import software.amazon.awssdk.services.kafkaconnect.model.CreateConnectorResponse;
import software.amazon.awssdk.services.kafkaconnect.model.CustomPlugin;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeConnectorRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeConnectorResponse;
import software.amazon.awssdk.services.kafkaconnect.model.KafkaCluster;
import software.amazon.awssdk.services.kafkaconnect.model.KafkaClusterClientAuthentication;
import software.amazon.awssdk.services.kafkaconnect.model.KafkaClusterClientAuthenticationType;
import software.amazon.awssdk.services.kafkaconnect.model.KafkaClusterEncryptionInTransit;
import software.amazon.awssdk.services.kafkaconnect.model.KafkaClusterEncryptionInTransitType;
import software.amazon.awssdk.services.kafkaconnect.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.kafkaconnect.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.kafkaconnect.model.Plugin;
import software.amazon.awssdk.services.kafkaconnect.model.ProvisionedCapacity;
import software.amazon.awssdk.services.kafkaconnect.model.Vpc;
import software.amazon.cloudformation.exceptions.CfnAlreadyExistsException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnResourceConflictException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CreateHandlerTest extends AbstractTestBase {

    @Mock
    private KafkaConnectClient kafkaConnectClient;

    @Mock
    private ExceptionTranslator exceptionTranslator;

    @Mock
    private Translator translator;

    private ReadHandler readHandler;

    private AmazonWebServicesClientProxy proxy;

    private ProxyClient<KafkaConnectClient> proxyClient;

    private CreateHandler handler;

    @BeforeEach
    public void setup() {
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS,
            () -> Duration.ofSeconds(600).toMillis());
        proxyClient = proxyStub(proxy, kafkaConnectClient);
        readHandler = new ReadHandler(exceptionTranslator, translator);
        handler = new CreateHandler(exceptionTranslator, translator, readHandler);
    }

    @AfterEach
    public void tear_down() {
        verify(kafkaConnectClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(kafkaConnectClient, exceptionTranslator, translator);
    }

    @Test
    public void handleRequest_success() {
        final ResourceModel resourceModel = TestData.getResourceModel();
        when(translator.translateToCreateRequest(resourceModel, TagHelper.convertToMap(resourceModel.getTags())))
            .thenReturn(TestData.CREATE_CONNECTOR_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.CREATE_CONNECTOR_REQUEST, kafkaConnectClient::createConnector)
        ).thenReturn(TestData.CREATE_CONNECTOR_RESPONSE);
        when(translator.translateToReadRequest(TestData.RESOURCE_MODEL_WITH_ARN))
            .thenReturn(TestData.DESCRIBE_CONNECTOR_REQUEST);
        final DescribeConnectorResponse describeConnectorResponse =
            TestData.describeResponseWithState(ConnectorState.RUNNING);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DESCRIBE_CONNECTOR_REQUEST, kafkaConnectClient::describeConnector
        )).thenReturn(describeConnectorResponse);
        when(translator.translateFromReadResponse(describeConnectorResponse))
            .thenReturn(TestData.RESOURCE_MODEL_WITH_ARN);
        when(proxyClient.injectCredentialsAndInvokeV2(TestData.LIST_TAGS_FOR_RESOURCE_REQUEST,
                kafkaConnectClient::listTagsForResource)).thenReturn(TestData.LIST_TAGS_FOR_RESOURCE_RESPONSE);

        final ResourceHandlerRequest<ResourceModel> request = TestData.getResourceHandlerRequest(resourceModel);
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(
                proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isEqualTo(TestData.DESCRIBE_RESPONSE);
        assertThat(response.getResourceModel().getTags())
                .isEqualTo(request.getDesiredResourceState().getTags());
    }

    @Test
    public void handleRequest_afterNDescribeConnectors_success() {
        final ResourceModel resourceModel = TestData.getResourceModel();
        when(translator.translateToCreateRequest(resourceModel, TagHelper.convertToMap(resourceModel.getTags())))
            .thenReturn(TestData.CREATE_CONNECTOR_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.CREATE_CONNECTOR_REQUEST, kafkaConnectClient::createConnector)
        ).thenReturn(TestData.CREATE_CONNECTOR_RESPONSE);
        when(translator.translateToReadRequest(TestData.RESOURCE_MODEL_WITH_ARN))
            .thenReturn(TestData.DESCRIBE_CONNECTOR_REQUEST);
        final DescribeConnectorResponse describeRunningConnectorResponse =
            TestData.describeResponseWithState(ConnectorState.RUNNING);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DESCRIBE_CONNECTOR_REQUEST, kafkaConnectClient::describeConnector))
                .thenReturn(TestData.describeResponseWithState(ConnectorState.CREATING))
                .thenReturn(TestData.describeResponseWithState(ConnectorState.CREATING))
                .thenReturn(describeRunningConnectorResponse);
        when(translator.translateFromReadResponse(describeRunningConnectorResponse))
            .thenReturn(TestData.RESOURCE_MODEL_WITH_ARN);
        when(proxyClient.injectCredentialsAndInvokeV2(TestData.LIST_TAGS_FOR_RESOURCE_REQUEST,
                kafkaConnectClient::listTagsForResource)).thenReturn(TestData.LIST_TAGS_FOR_RESOURCE_RESPONSE);

        final ResourceHandlerRequest<ResourceModel> request = TestData.getResourceHandlerRequest(resourceModel);
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(
                proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isEqualTo(TestData.DESCRIBE_RESPONSE);
        assertThat(response.getResourceModel().getTags())
                .isEqualTo(request.getDesiredResourceState().getTags());
    }

    @Test
    public void handleRequest_throwsAlreadyExistsException_whenConnectorExists() {
        final ResourceModel resourceModel = TestData.getResourceModel();
        final ConflictException cException = ConflictException.builder().build();
        final CfnAlreadyExistsException cfnException = new CfnAlreadyExistsException(cException);
        when(translator.translateToCreateRequest(resourceModel, TagHelper.convertToMap(resourceModel.getTags())))
            .thenReturn(TestData.CREATE_CONNECTOR_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.CREATE_CONNECTOR_REQUEST,
            kafkaConnectClient::createConnector
        )).thenThrow(cException);
        when(exceptionTranslator.translateToCfnException(cException, TestData.CONNECTOR_NAME))
            .thenReturn(cfnException);

        final CfnAlreadyExistsException exception = assertThrows(CfnAlreadyExistsException.class,
            () -> handler.handleRequest(proxy, TestData.getResourceHandlerRequest(resourceModel),
                new CallbackContext(), proxyClient, logger));

        assertThat(exception).isEqualTo(cfnException);
    }

    @Test
    public void handleRequest_throwsGeneralServiceException_whenConnectorsStateFails() {
        setupMocksToReturnConnectorState(ConnectorState.FAILED);

        runHandlerAndAssertExceptionThrownWithMessage(CfnGeneralServiceException.class,
            "Error occurred during operation 'Couldn't create AWS::KafkaConnect::Connector " +
                "due to create failure'.");
    }

    @Test
    public void handleRequest_throwsResourceConflictException_whenConnectorStartsDeletingDuringCreate() {
        setupMocksToReturnConnectorState(ConnectorState.DELETING);

        runHandlerAndAssertExceptionThrownWithMessage(CfnResourceConflictException.class,
            "Resource of type 'AWS::KafkaConnect::Connector' with identifier '" + TestData.CONNECTOR_ARN
                + "' has a conflict. Reason: Another process is deleting this AWS::KafkaConnect::Connector.");
    }

    @Test
    public void handleRequest_throwsResourceConflictException_whenConnectorReturnsUpdating() {
        setupMocksToReturnConnectorState(ConnectorState.UPDATING);

        runHandlerAndAssertExceptionThrownWithMessage(CfnResourceConflictException.class,
            "Resource of type 'AWS::KafkaConnect::Connector' with identifier '" + TestData.CONNECTOR_ARN
                + "' has a conflict. Reason: Another process is updating this AWS::KafkaConnect::Connector.");
    }

    @Test
    public void handleRequest_throwsGeneralServiceException_whenConnectorReturnsUnexpectedState() {
        setupMocksToReturnConnectorState(ConnectorState.UNKNOWN_TO_SDK_VERSION);

        runHandlerAndAssertExceptionThrownWithMessage(CfnGeneralServiceException.class,
            "Error occurred during operation 'AWS::KafkaConnect::Connector create request accepted " +
                "but current state is unknown'.");
    }

    @Test
    public void handleRequest_throwsGeneralServiceException_whenDescribeConnectorThrowsException() {
        final AwsServiceException cException = AwsServiceException.builder()
            .message(TestData.EXCEPTION_MESSAGE).build();
        when(translator.translateToCreateRequest(TestData.getResourceModel(),
            TagHelper.convertToMap(TestData.getResourceModel().getTags())))
                .thenReturn(TestData.CREATE_CONNECTOR_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.CREATE_CONNECTOR_REQUEST, kafkaConnectClient::createConnector)
        ).thenReturn(TestData.CREATE_CONNECTOR_RESPONSE);
        when(translator.translateToReadRequest(TestData.RESOURCE_MODEL_WITH_ARN))
            .thenReturn(TestData.DESCRIBE_CONNECTOR_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DESCRIBE_CONNECTOR_REQUEST, kafkaConnectClient::describeConnector
        )).thenThrow(cException);

        runHandlerAndAssertExceptionThrownWithMessage(CfnGeneralServiceException.class,
            "Error occurred during operation 'AWS::KafkaConnect::Connector create request accepted " +
                "but failed to get state due to: " + TestData.EXCEPTION_MESSAGE + "'.");
    }

    private void setupMocksToReturnConnectorState(final ConnectorState connectorState) {
        when(translator.translateToCreateRequest(TestData.RESOURCE_MODEL, TagHelper.convertToMap(TestData.RESOURCE_MODEL.getTags())))
            .thenReturn(TestData.CREATE_CONNECTOR_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.CREATE_CONNECTOR_REQUEST, kafkaConnectClient::createConnector)
        ).thenReturn(TestData.CREATE_CONNECTOR_RESPONSE);
        when(translator.translateToReadRequest(TestData.RESOURCE_MODEL_WITH_ARN))
            .thenReturn(TestData.DESCRIBE_CONNECTOR_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DESCRIBE_CONNECTOR_REQUEST, kafkaConnectClient::describeConnector
        )).thenReturn(TestData.describeResponseWithState(connectorState));
    }

    private void runHandlerAndAssertExceptionThrownWithMessage(
        final Class<? extends Exception> expectedExceptionClass, final String expectedMessage) {

        final Exception exception = assertThrows(expectedExceptionClass, () ->
            handler.handleRequest(
                proxy,
                TestData.getResourceHandlerRequest(TestData.getResourceModel()),
                new CallbackContext(), proxyClient, logger)
        );

        assertThat(exception.getMessage()).isEqualTo(expectedMessage);
    }

    private static class TestData {
        private static final String CONNECTOR_NAME = "unit-test-connector";
        private static final String CONNECTOR_ARN =
            "arn:aws:kafkaconnect:us-east-1:123456789:connector/unit-test-connector";
        private static final Map<String, String> CONNECTOR_CONFIGURATION = new HashMap<String, String>() {{
            put("tasks.max", "2");
            put("connector.class", "io.confluent.connect.s3.S3SinkConnector");
        }};
        private static final String KAFKA_CONNECT_VERSION = "2.7.1";
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
        private static final String EXCEPTION_MESSAGE = "Exception message";
        private static final String AUTHENTICATION_TYPE = KafkaClusterClientAuthenticationType.NONE.toString();
        private static final String ENCRYPTION_TYPE = KafkaClusterEncryptionInTransitType.PLAINTEXT.toString();

        private static final ResourceModel RESOURCE_MODEL = ResourceModel
            .builder()
            .connectorName(CONNECTOR_NAME)
            .tags(TagHelper.convertToSet(TAGS))
            .build();

        private static final CreateConnectorRequest CREATE_CONNECTOR_REQUEST =
            CreateConnectorRequest.builder()
                .capacity(Capacity.builder()
                    .provisionedCapacity(ProvisionedCapacity
                        .builder()
                        .workerCount(WORKER_COUNT)
                        .mcuCount(MCU_COUNT)
                        .build())
                    .build())
                .connectorConfiguration(CONNECTOR_CONFIGURATION)
                .connectorName(CONNECTOR_NAME)
                .kafkaCluster(KafkaCluster.builder()
                    .apacheKafkaCluster(ApacheKafkaCluster
                        .builder()
                        .bootstrapServers(KAFKA_CLUSTER_BOOTSTRAP_SERVERS)
                        .vpc(Vpc.builder()
                            .subnets(Arrays.asList(VPC_SUBNET_1, VPC_SUBNET_2))
                            .securityGroups(Arrays.asList(VPC_SECURITY_GROUP))
                            .build())
                        .build())
                    .build())
                .kafkaClusterClientAuthentication(KafkaClusterClientAuthentication.builder()
                    .authenticationType(AUTHENTICATION_TYPE)
                    .build())
                .kafkaClusterEncryptionInTransit(KafkaClusterEncryptionInTransit.builder()
                    .encryptionType(ENCRYPTION_TYPE)
                    .build())
                .kafkaConnectVersion(KAFKA_CONNECT_VERSION)
                .plugins(Arrays.asList(Plugin.builder()
                    .customPlugin(CustomPlugin.builder()
                        .customPluginArn(CUSTOM_PLUGIN_ARN)
                        .revision(CUSTOM_PLUGIN_REVISION)
                        .build())
                    .build()))
                .serviceExecutionRoleArn(SERVICE_EXECUTION_ROLE_ARN)
                .tags(TAGS)
                .build();

        private static final CreateConnectorResponse CREATE_CONNECTOR_RESPONSE =
            CreateConnectorResponse
                .builder()
                .connectorArn(CONNECTOR_ARN)
                .connectorName(CONNECTOR_NAME)
                .connectorState(ConnectorState.CREATING)
                .build();

        private static final DescribeConnectorRequest DESCRIBE_CONNECTOR_REQUEST =
            DescribeConnectorRequest.builder()
                .connectorArn(CONNECTOR_ARN)
                .build();

        private static final ResourceModel RESOURCE_MODEL_WITH_ARN = ResourceModel.builder()
            .connectorName(CONNECTOR_NAME)
            .connectorArn(CONNECTOR_ARN)
            .tags(TagHelper.convertToSet(TAGS))
            .build();

        private static final ProgressEvent<ResourceModel, CallbackContext> DESCRIBE_RESPONSE =
            ProgressEvent.<ResourceModel, CallbackContext>builder()
                .resourceModel(RESOURCE_MODEL_WITH_ARN)
                .status(OperationStatus.SUCCESS)
                .build();

        private static final ResourceHandlerRequest<ResourceModel> getResourceHandlerRequest(
            final ResourceModel resourceModel) {

            return ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(resourceModel)
                .desiredResourceTags(TAGS)
                .build();
        }

        private static final ResourceModel getResourceModel() {
            return ResourceModel
                .builder()
                .connectorName(CONNECTOR_NAME)
                .tags(TagHelper.convertToSet(TAGS))
                .build();
        }

        private static DescribeConnectorResponse describeResponseWithState(final ConnectorState state) {
            return DescribeConnectorResponse
                .builder()
                .connectorArn(CONNECTOR_ARN)
                .connectorName(CONNECTOR_NAME)
                .connectorState(state)
                .build();
        }

        private static final ListTagsForResourceRequest LIST_TAGS_FOR_RESOURCE_REQUEST =
                ListTagsForResourceRequest.builder()
                        .resourceArn(CONNECTOR_ARN)
                        .build();

        private static final ListTagsForResourceResponse LIST_TAGS_FOR_RESOURCE_RESPONSE =
                ListTagsForResourceResponse.builder()
                        .tags(TAGS)
                        .build();
    }
}
