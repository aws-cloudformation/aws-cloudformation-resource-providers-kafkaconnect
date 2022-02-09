package software.amazon.kafkaconnect.connector;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.kafkaconnect.KafkaConnectClient;
import software.amazon.awssdk.services.kafkaconnect.model.ApacheKafkaClusterDescription;
import software.amazon.awssdk.services.kafkaconnect.model.CapacityDescription;
import software.amazon.awssdk.services.kafkaconnect.model.CapacityUpdate;
import software.amazon.awssdk.services.kafkaconnect.model.ConnectorState;
import software.amazon.awssdk.services.kafkaconnect.model.CustomPluginDescription;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeConnectorRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeConnectorResponse;
import software.amazon.awssdk.services.kafkaconnect.model.KafkaClusterClientAuthenticationDescription;
import software.amazon.awssdk.services.kafkaconnect.model.KafkaClusterDescription;
import software.amazon.awssdk.services.kafkaconnect.model.KafkaClusterEncryptionInTransitDescription;
import software.amazon.awssdk.services.kafkaconnect.model.NotFoundException;
import software.amazon.awssdk.services.kafkaconnect.model.PluginDescription;
import software.amazon.awssdk.services.kafkaconnect.model.ProvisionedCapacityDescription;
import software.amazon.awssdk.services.kafkaconnect.model.ProvisionedCapacityUpdate;
import software.amazon.awssdk.services.kafkaconnect.model.UpdateConnectorRequest;
import software.amazon.awssdk.services.kafkaconnect.model.UpdateConnectorResponse;
import software.amazon.awssdk.services.kafkaconnect.model.VpcDescription;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.exceptions.CfnNotUpdatableException;
import software.amazon.cloudformation.exceptions.CfnResourceConflictException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class UpdateHandlerTest extends AbstractTestBase {

    @Mock
    private KafkaConnectClient kafkaConnectClient;

    @Mock
    private ExceptionTranslator exceptionTranslator;

    @Mock
    private Translator translator;

    private ReadHandler readHandler;

    private AmazonWebServicesClientProxy proxy;

    private ProxyClient<KafkaConnectClient> proxyClient;

    private UpdateHandler handler;

    @BeforeEach
    public void setup() {
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS,
            () -> Duration.ofSeconds(600).toMillis());
        proxyClient = proxyStub(proxy, kafkaConnectClient);
        readHandler = new ReadHandler(exceptionTranslator, translator);
        handler = new UpdateHandler(exceptionTranslator, translator, readHandler);
    }

    @AfterEach
    public void tear_down() {
        verify(kafkaConnectClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(kafkaConnectClient, translator, exceptionTranslator);
    }

    @Test
    public void handleRequest_success() {
        final ResourceModel requestResourceModel = TestData.resourceModelWithCapacity(
            TestData.updatedCapacityOnlyProvisionedCapacity());
        runSuccessfulHandleRequest(requestResourceModel);
    }

    @Test
    public void handlerRequest_whenArrayOrderIsDifferent_success() {
        final ResourceModel requestResourceModel = TestData.resourceModel(TestData.CONNECTOR_NAME,
            TestData.updatedCapacityOnlyProvisionedCapacity(), TestData.KAFKA_CLUSTER_DIFFERENT_ORDER);
        runSuccessfulHandleRequest(requestResourceModel);
    }

    @Test
    public void handleRequest_afterNRoundsOfUpdatingStabilization_success() {
        final ResourceModel requestResourceModel =
            TestData.resourceModelWithCapacity(TestData.updatedCapacityOnlyProvisionedCapacity());
        final DescribeConnectorResponse unchangedDescribeConnectorResponse = TestData
            .unchangedDescribeConnectorResponse(TestData.unchangedCapacityDescription(), ConnectorState.RUNNING);
        final ResourceModel unchangedConnector = TestData.resourceModelWithCapacity(TestData.unchangedCapacity());
        final ResourceModel updatedResourceModel =
            TestData.resourceModelWithCapacity(TestData.updatedCapacityOnlyProvisionedCapacity());
        final DescribeConnectorResponse runningDescribeConnectorResponse = TestData
            .unchangedDescribeConnectorResponse(TestData.capacityDescriptionOnlyProvisionedCapacity(),
                ConnectorState.RUNNING);
        final DescribeConnectorResponse updatingDescribeConnectorResponse = TestData
            .updatedDescribeConnectorResponse(TestData.capacityDescriptionOnlyProvisionedCapacity(),
                ConnectorState.UPDATING);
        setupMocksForUpdateConnectorSuccess(requestResourceModel);
        setupTranslateFromReadMockWithMultipleInputs(
            asList(unchangedDescribeConnectorResponse, runningDescribeConnectorResponse),
            asList(unchangedConnector, updatedResourceModel));
        when(translator.translateToReadRequest(requestResourceModel))
            .thenReturn(TestData.describeConnectorRequest());
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.describeConnectorRequest(),
            kafkaConnectClient::describeConnector)
        )
            .thenReturn(unchangedDescribeConnectorResponse)
            .thenReturn(unchangedDescribeConnectorResponse)
            .thenReturn(updatingDescribeConnectorResponse)
            .thenReturn(updatingDescribeConnectorResponse)
            .thenReturn(runningDescribeConnectorResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy,
            TestData.resourceHandlerRequest(), new CallbackContext(), proxyClient, logger);

        final ProgressEvent<ResourceModel, CallbackContext> expected =
            TestData.describeResponse(updatedResourceModel);
        assertThat(response).isEqualTo(expected);
    }

    @Test
    public void handlerRequest_throwsCfnDoNotExistException_whenConnectorDoesNotExist() {
        final NotFoundException serviceException = NotFoundException.builder().build();
        final CfnNotFoundException cfnException = new CfnNotFoundException(serviceException);
        setupDescribeMocksToThrowException(serviceException);
        when(exceptionTranslator.translateToCfnException(serviceException, TestData.CONNECTOR_ARN))
            .thenReturn(cfnException);

        final CfnNotFoundException exception = assertThrows(CfnNotFoundException.class, () -> handler
            .handleRequest(proxy, TestData.resourceHandlerRequest(), new CallbackContext(), proxyClient, logger));

        assertThat(exception).isEqualTo(cfnException);
    }

    @Test
    public void handlerRequest_throwsCfnGeneralServiceException_whenDescribeConnectorFails() {
        setupDescribeMocksToThrowException(AwsServiceException.builder()
            .message(TestData.EXCEPTION_MESSAGE).build());

        runHandlerAndAssertExceptionThrownWithMessage(TestData.resourceHandlerRequest(),
            CfnGeneralServiceException.class, String.format("Error occurred during operation 'Could not update %s " +
                "due to read failure from %s'.", ResourceModel.TYPE_NAME, TestData.EXCEPTION_MESSAGE));
    }

    @Test
    public void handlerRequest_throwsCfnException_whenUpdateConnectorFails() {
        final AwsServiceException serviceException = AwsServiceException.builder().build();
        final CfnGeneralServiceException cfnException = new CfnGeneralServiceException(serviceException);
        final ResourceModel resourceModel = TestData.resourceModelWithCapacity(
            TestData.updatedCapacityOnlyProvisionedCapacity());
        final ResourceModel unchangedConnector = TestData.resourceModelWithCapacity(TestData.unchangedCapacity());
        final UpdateConnectorRequest updateConnectorRequest =
            TestData.updateConnectorRequest(TestData.capacityUpdateOnlyProvisionedCapacity());
        setupDescribeMocksForSuccess(resourceModel, unchangedConnector);
        when(translator.translateToUpdateRequest(resourceModel)).thenReturn(updateConnectorRequest);
        when(proxyClient.injectCredentialsAndInvokeV2(
            updateConnectorRequest,
            kafkaConnectClient::updateConnector)
        ).thenThrow(serviceException);
        when(exceptionTranslator.translateToCfnException(serviceException, TestData.CONNECTOR_ARN))
            .thenReturn(cfnException);

        final CfnGeneralServiceException exception = assertThrows(CfnGeneralServiceException.class, () -> handler
            .handleRequest(proxy, TestData.resourceHandlerRequest(), new CallbackContext(), proxyClient, logger));

        assertThat(exception).isEqualTo(cfnException);
    }

    @Test
    public void handleRequest_throwsGeneralServiceException_whenConnectorStateFailed() {
        setupMocksToReturnConnectorState(ConnectorState.FAILED);

        runHandlerAndAssertExceptionThrownWithMessage(TestData.resourceHandlerRequest(),
            CfnGeneralServiceException.class, String.format("Error occurred during operation " +
                "'Couldn't update %s due to update failure'.", ResourceModel.TYPE_NAME));
    }

    @Test
    public void handleRequest_throwsResourceConflictException_whenConnectorStateDeleting() {
        setupMocksToReturnConnectorState(ConnectorState.DELETING);

        runHandlerAndAssertExceptionThrownWithMessage(TestData.resourceHandlerRequest(),
            CfnResourceConflictException.class, String.format("Resource of type '%s' with identifier '%s' " +
                "has a conflict. Reason: Another process is deleting this %s.",
                ResourceModel.TYPE_NAME, TestData.CONNECTOR_ARN, ResourceModel.TYPE_NAME));
    }

    @Test
    public void handleRequest_throwsResourceConflictException_whenConnectorStateCreating() {
        setupMocksToReturnConnectorState(ConnectorState.CREATING);

        runHandlerAndAssertExceptionThrownWithMessage(TestData.resourceHandlerRequest(),
            CfnResourceConflictException.class, String.format("Resource of type '%s' with identifier '%s' " +
                "has a conflict. Reason: This %s is being created and cannot be updated.",
                ResourceModel.TYPE_NAME, TestData.CONNECTOR_ARN, ResourceModel.TYPE_NAME));
    }

    @Test
    public void handleRequest_throwsGeneralServiceException_whenConnectorStateUnknown() {
        setupMocksToReturnConnectorState(ConnectorState.UNKNOWN_TO_SDK_VERSION);

        runHandlerAndAssertExceptionThrownWithMessage(TestData.resourceHandlerRequest(),
            CfnGeneralServiceException.class, String.format("Error occurred during operation '%s update " +
                "request accepted but current state is unknown'.", ResourceModel.TYPE_NAME));
    }

    @Test
    public void handleRequest_throwsCfnGeneralServiceException_whenUpdateProvisionedCapacityReverts() {
        final ResourceModel requestResourceModel = TestData.resourceModelWithCapacity(TestData
            .updatedCapacityOnlyProvisionedCapacity());
        final ResourceModel unchangedDescribeResponseTranslatedToResourceModel = TestData
            .resourceModelWithCapacity(TestData.UNCHANGED_CAPACITY_ONLY_PROVISIONED_CAPACITY);
        final ResourceModel resourceModelWithCurrentVersion = TestData
            .resourceModelWithCapacity(TestData.updatedCapacityOnlyProvisionedCapacity());
        setupDescribeMocksWithMultipleInputsWithOutput(requestResourceModel, resourceModelWithCurrentVersion,
            unchangedDescribeResponseTranslatedToResourceModel);
        setupMocksForUpdateConnectorSuccess(resourceModelWithCurrentVersion);

        runHandlerAndAssertExceptionThrownWithMessage(TestData.resourceHandlerRequest(),
            CfnGeneralServiceException.class, String.format("Error occurred during operation 'Couldn't update " +
                "%s due to update failure. Resource reverted to previous state'.", ResourceModel.TYPE_NAME));
    }

    @Test
    public void handleRequest_throwsCfnGeneralServiceException_whenUpdateAutoScalingReverts() {
        final ResourceModel requestResourceModel = TestData.resourceModelWithCapacity(TestData.capacityOnlyAutoscaling());
        final ResourceModel unchangedDescribeResponseTranslatedToResourceModel = TestData
            .resourceModelWithCapacity(TestData.UNCHANGED_CAPACITY_ONLY_AUTOSCALING);
        final ResourceModel resourceModelWithCurrentVersion = TestData
            .resourceModelWithCapacity(TestData.capacityOnlyAutoscaling());
        setupMocksForUpdateConnectorSuccess(requestResourceModel);
        setupDescribeMocksWithMultipleInputsWithOutput(requestResourceModel, resourceModelWithCurrentVersion,
            unchangedDescribeResponseTranslatedToResourceModel);

        runHandlerAndAssertExceptionThrownWithMessage(TestData.RESOURCE_HANDLER_REQUEST_AUTOSCALING,
            CfnGeneralServiceException.class, String.format("Error occurred during operation 'Couldn't " +
                "update %s due to update failure. Resource reverted to previous state'.", ResourceModel.TYPE_NAME));
    }

    @Test
    public void handleRequest_throwsCfnNotUpdatableException_whenConnectorNotRunning() {
        final ResourceModel resourceModel = TestData.resourceModelWithCapacity(TestData
            .updatedCapacityOnlyProvisionedCapacity());
        final DescribeConnectorResponse describeConnectorResponse = TestData.unchangedDescribeConnectorResponse(
            TestData.capacityDescriptionOnlyProvisionedCapacity(), ConnectorState.FAILED);
        final DescribeConnectorRequest describeConnectorRequest = TestData.describeConnectorRequest();
        when(translator.translateToReadRequest(resourceModel)).thenReturn(describeConnectorRequest);
        when(proxyClient.injectCredentialsAndInvokeV2(
            describeConnectorRequest,
            kafkaConnectClient::describeConnector)
        ).thenReturn(describeConnectorResponse);

        runHandlerAndAssertExceptionThrownWithMessage(TestData.resourceHandlerRequest(),
            CfnNotUpdatableException.class, String.format("Resource of type '%s' with identifier '%s' is not " +
                "updatable with parameters provided.", ResourceModel.TYPE_NAME, TestData.CONNECTOR_ARN));
    }

    @Test
    public void handlerRequest_throwsCfnNotUpdatableException_whenUpdateCreateOnlyProperty() {
        setupDescribeMocksForSuccess(TestData.resourceModelWithName(TestData.CONNECTOR_NAME),
            TestData.resourceModelWithName(TestData.CONNECTOR_NAME_CHANGE));

        runHandlerAndAssertExceptionThrownWithMessage(TestData.CREATE_ONLY_UPDATE_HANDLER_REQUEST,
            CfnNotUpdatableException.class, String.format("Resource of type '%s' with identifier '%s' is not " +
                "updatable with parameters provided.", ResourceModel.TYPE_NAME, TestData.CONNECTOR_ARN));
    }

    private void setupMocksToReturnConnectorState(final ConnectorState connectorState) {
        final ResourceModel resourceModel = TestData.resourceModelWithCapacity(TestData
            .updatedCapacityOnlyProvisionedCapacity());
        final DescribeConnectorResponse unchangedDescribeConnectorResponse = TestData
            .unchangedDescribeConnectorResponse(TestData.unchangedCapacityDescription(), ConnectorState.RUNNING);
        final ResourceModel unchangedConnector = TestData.resourceModelWithCapacity(TestData.unchangedCapacity());
        final DescribeConnectorRequest describeConnectorRequest = TestData.describeConnectorRequest();
        when(translator.translateFromReadResponse(unchangedDescribeConnectorResponse)).thenReturn(unchangedConnector);
        when(translator.translateToReadRequest(resourceModel)).thenReturn(describeConnectorRequest);
        setupMocksForUpdateConnectorSuccess(resourceModel);
        when(proxyClient.injectCredentialsAndInvokeV2(
            describeConnectorRequest,
            kafkaConnectClient::describeConnector)
        )
            .thenReturn(unchangedDescribeConnectorResponse)
            .thenReturn(TestData.unchangedDescribeConnectorResponse(
                TestData.capacityDescriptionOnlyProvisionedCapacity(), connectorState));
    }

    private void setupMocksForUpdateConnectorSuccess(final ResourceModel resourceModel) {
        final UpdateConnectorRequest updateConnectorRequest =
            TestData.updateConnectorRequest(TestData.capacityUpdateOnlyProvisionedCapacity());
        when(translator.translateToUpdateRequest(resourceModel)).thenReturn(updateConnectorRequest);
        when(proxyClient.injectCredentialsAndInvokeV2(
            updateConnectorRequest,
            kafkaConnectClient::updateConnector)
        ).thenReturn(TestData.updateConnectorResponse());
    }

    private void setupDescribeMocksForSuccess(final ResourceModel input, final ResourceModel output) {
        final DescribeConnectorResponse describeConnectorResponse = TestData.unchangedDescribeConnectorResponse(
            TestData.capacityDescriptionOnlyProvisionedCapacity(), ConnectorState.RUNNING);
        final DescribeConnectorRequest describeConnectorRequest = TestData.describeConnectorRequest();
        when(translator.translateToReadRequest(input)).thenReturn(describeConnectorRequest);
        when(proxyClient.injectCredentialsAndInvokeV2(
            describeConnectorRequest,
            kafkaConnectClient::describeConnector)
        ).thenReturn(describeConnectorResponse);
        when(translator.translateFromReadResponse(describeConnectorResponse)).thenReturn(output);
    }

    private void setupDescribeMocksToThrowException(final AwsServiceException serviceException) {
        final ResourceModel resourceModel = TestData.resourceModelWithCapacity(TestData
            .updatedCapacityOnlyProvisionedCapacity());
        final DescribeConnectorRequest describeConnectorRequest = TestData.describeConnectorRequest();
        when(translator.translateToReadRequest(resourceModel)).thenReturn(describeConnectorRequest);
        when(proxyClient.injectCredentialsAndInvokeV2(
            describeConnectorRequest,
            kafkaConnectClient::describeConnector)
        ).thenThrow(serviceException);
    }

    private void setupDescribeMocksWithMultipleInputsWithOutput(final ResourceModel input1,
        final ResourceModel input2, final ResourceModel output) {

        final DescribeConnectorRequest describeConnectorRequest = TestData.describeConnectorRequest();
        final DescribeConnectorResponse unchangedDescribeConnectorResponse = TestData
            .unchangedDescribeConnectorResponse(TestData.unchangedCapacityDescription(),
                ConnectorState.RUNNING);
        setupTranslateToReadMockWithMultipleInputs(input1, input2, describeConnectorRequest);
        when(proxyClient.injectCredentialsAndInvokeV2(
            describeConnectorRequest,
            kafkaConnectClient::describeConnector
        )).thenReturn(unchangedDescribeConnectorResponse);
        when(translator.translateFromReadResponse(unchangedDescribeConnectorResponse)).thenReturn(
            output);
    }

    private void setupTranslateToReadMockWithMultipleInputs(final ResourceModel input1,
        final ResourceModel input2, final DescribeConnectorRequest describeConnectorRequest) {

        when(translator.translateToReadRequest(any(ResourceModel.class))).thenAnswer(
            invocationOnMock -> {
                if(Objects.equals(invocationOnMock.getArguments()[0], input1) ||
                    Objects.equals(invocationOnMock.getArguments()[0], input2)) {
                    return describeConnectorRequest;
                }
                throw new Exception();
            }
        );
    }

    private void setupTranslateFromReadMockWithMultipleInputs(
        final List<DescribeConnectorResponse> responses,
        final List<ResourceModel> resourceModels) {

        when(translator.translateFromReadResponse(any(DescribeConnectorResponse.class))).thenAnswer(
            invocationOnMock -> {
                for (int i = 0; i < responses.size(); ++i) {
                    if(invocationOnMock.getArguments()[0] == responses.get(i)) {
                        return resourceModels.get(i);
                    }
                }
                throw new Exception();
            }
        );
    }

    private void runHandlerAndAssertExceptionThrownWithMessage(
        final ResourceHandlerRequest<ResourceModel> resourceHandlerRequest,
        final Class<? extends Exception> expectedExceptionClass,
        final String expectedMessage) {

        final Exception exception = assertThrows(expectedExceptionClass, () ->
            handler.handleRequest(proxy, resourceHandlerRequest, new CallbackContext(), proxyClient, logger));

        assertThat(exception.getMessage()).isEqualTo(expectedMessage);
    }

    private void runSuccessfulHandleRequest(final ResourceModel requestResourceModel) {
        final DescribeConnectorResponse unchangedDescribeConnectorResponse =
            TestData.unchangedDescribeConnectorResponse(TestData.capacityDescriptionOnlyProvisionedCapacity(),
                ConnectorState.RUNNING);
        final ResourceModel unchangedDescribeResponseTranslatedToResourceModel =
            TestData.resourceModelWithCapacity(TestData.updatedCapacityOnlyProvisionedCapacity());
        final DescribeConnectorResponse updatedDescribeConnectorResponse =
            TestData.updatedDescribeConnectorResponse(TestData.capacityDescriptionOnlyProvisionedCapacity(),
                ConnectorState.RUNNING);
        final ResourceModel updatedResourceModel =
            TestData.resourceModelWithCapacity(TestData.updatedCapacityOnlyProvisionedCapacity());
        final DescribeConnectorRequest describeConnectorRequest = TestData.describeConnectorRequest();
        setupTranslateToReadMockWithMultipleInputs(requestResourceModel,
            unchangedDescribeResponseTranslatedToResourceModel, describeConnectorRequest);
        when(proxyClient.injectCredentialsAndInvokeV2(
            describeConnectorRequest,
            kafkaConnectClient::describeConnector
        ))
            .thenReturn(unchangedDescribeConnectorResponse)
            .thenReturn(unchangedDescribeConnectorResponse)
            .thenReturn(updatedDescribeConnectorResponse);
        setupTranslateFromReadMockWithMultipleInputs(
            asList(unchangedDescribeConnectorResponse, updatedDescribeConnectorResponse),
            asList(unchangedDescribeResponseTranslatedToResourceModel, updatedResourceModel));
        setupMocksForUpdateConnectorSuccess(unchangedDescribeResponseTranslatedToResourceModel);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy,
            TestData.resourceHandlerRequest(), new CallbackContext(), proxyClient, logger);

        final ProgressEvent<ResourceModel, CallbackContext> expected =
            TestData.describeResponse(updatedResourceModel);
        assertThat(response).isEqualTo(expected);
    }

    private static class TestData {
        private static final int WORKER_COUNT = 2;
        private static final int UNCHANGED_WORKER_COUNT = 4;
        private static final int MCU_COUNT = 2;
        private static final int MIN_WORKER_COUNT = 1;
        private static final int MAX_WORKER_COUNT = 10;
        private static final int SCALE_OUT_UTIL_PERCENT = 70;
        private static final int SCALE_IN_UTIL_PERCENT = 50;
        private static final String CONNECTOR_ARN =
            "arn:aws:kafkaconnect:us-east-1:123456789:connector/unit-test-connector";
        private static final String CONNECTOR_NAME = "unit-test-connector";
        private static final String CONNECTOR_NAME_CHANGE = "updating-create-only-property";
        private static final String UNCHANGED_CURRENT_VERSION = "AB1CDQEFGHZ5";
        private static final String UPDATED_CURRENT_VERSION = "ZB1EGIEFGHL5";
        private static final String EXCEPTION_MESSAGE = "Exception message";
        private static final Map<String, String> CONNECTOR_CONFIGURATION = new HashMap<String, String>() {{
            put("tasks.max", "2");
            put("connector.class", "io.confluent.connect.s3.S3SinkConnector");
        }};
        private static final String KAFKA_CONNECT_VERSION = "2.7.1";
        private static final String BOOTSTRAP_SERVERS = "bootstrapServers";
        private static final List<String> SUBNETS = asList("subnet1", "subnet2");
        private static final List<String> SUBNETS_DIFFERENT_ORDER = asList("subnet2", "subnet1");
        private static final List<String> SECURITY_GROUPS = asList("securityGroup1", "securityGroup2");
        private static final KafkaCluster KAFKA_CLUSTER = KafkaCluster.builder()
            .apacheKafkaCluster(ApacheKafkaCluster.builder()
                .bootstrapServers(BOOTSTRAP_SERVERS)
                .vpc(Vpc.builder()
                    .subnets(new HashSet<>(SUBNETS))
                    .securityGroups(new HashSet<>(SECURITY_GROUPS))
                    .build())
                .build())
            .build();
        private static final KafkaCluster KAFKA_CLUSTER_DIFFERENT_ORDER = KafkaCluster.builder()
            .apacheKafkaCluster(ApacheKafkaCluster.builder()
                .bootstrapServers(BOOTSTRAP_SERVERS)
                .vpc(Vpc.builder()
                    .subnets(new HashSet<>(SUBNETS_DIFFERENT_ORDER))
                    .securityGroups(new HashSet<>(SECURITY_GROUPS))
                    .build())
                .build())
            .build();
        private static final KafkaClusterDescription KAFKA_CLUSTER_DESCRIPTION = KafkaClusterDescription.builder()
            .apacheKafkaCluster(ApacheKafkaClusterDescription.builder()
                .bootstrapServers(BOOTSTRAP_SERVERS)
                .vpc(VpcDescription.builder()
                    .subnets(SUBNETS)
                    .securityGroups(SECURITY_GROUPS)
                    .build())
                .build())
            .build();
        private static final String CLIENT_AUTHENTICATION_TYPE = "NONE";
        private static final KafkaClusterClientAuthentication KAFKA_CLUSTER_CLIENT_AUTHENTICATION =
            KafkaClusterClientAuthentication.builder()
                .authenticationType(CLIENT_AUTHENTICATION_TYPE)
                .build();
        private static final KafkaClusterClientAuthenticationDescription KAFKA_CLUSTER_AUTH_DESCRIPTION =
            KafkaClusterClientAuthenticationDescription.builder()
                .authenticationType(CLIENT_AUTHENTICATION_TYPE)
                .build();
        private static final String ENCRYPTION_IN_TRANSIT_TYPE = "PLAINTEXT";
        private static final KafkaClusterEncryptionInTransit KAFKA_CLUSTER_ENCRYPTION_IN_TRANSIT =
            KafkaClusterEncryptionInTransit.builder()
                .encryptionType(ENCRYPTION_IN_TRANSIT_TYPE)
                .build();
        private static final KafkaClusterEncryptionInTransitDescription KAFKA_CLUSTER_IN_TRANSIT_DESCRIPTION =
            KafkaClusterEncryptionInTransitDescription.builder()
                .encryptionType(ENCRYPTION_IN_TRANSIT_TYPE)
                .build();
        private static final String PLUGIN_ARN =
            "arn:aws:kafkaconnect:us-east-1:123456789:custom-plugin/unit-test-custom-plugin";
        private static final Long PLUGIN_REVISION = 1L;
        private static final List<Plugin> PLUGINS = asList(Plugin.builder()
            .customPlugin(CustomPlugin.builder()
                .customPluginArn(PLUGIN_ARN)
                .revision(PLUGIN_REVISION)
                .build())
            .build());
        private static final List<PluginDescription> PLUGIN_DESCRIPTIONS = asList(PluginDescription.builder()
            .customPlugin(CustomPluginDescription.builder()
                .customPluginArn(PLUGIN_ARN)
                .revision(PLUGIN_REVISION)
                .build())
            .build());
        private static final String SERVICE_EXECUTION_ROLE_ARN =
            "arn:aws:iam:us-east-1:123456789:iam-role/unit-test-service-execution-role";

        private static final Capacity UNCHANGED_CAPACITY_ONLY_PROVISIONED_CAPACITY = Capacity.builder()
            .provisionedCapacity(ProvisionedCapacity.builder()
                .workerCount(UNCHANGED_WORKER_COUNT)
                .mcuCount(MCU_COUNT)
                .build())
            .build();
        private static final Capacity UNCHANGED_CAPACITY_ONLY_AUTOSCALING = Capacity.builder()
            .build();
        private static final ResourceHandlerRequest<ResourceModel> RESOURCE_HANDLER_REQUEST_AUTOSCALING =
            ResourceHandlerRequest
                .<ResourceModel>builder()
                .desiredResourceState(resourceModelWithCapacity(capacityOnlyAutoscaling()))
                .build();
        private static final ResourceHandlerRequest<ResourceModel> CREATE_ONLY_UPDATE_HANDLER_REQUEST =
            ResourceHandlerRequest
                .<ResourceModel>builder()
                .desiredResourceState(resourceModelWithName(CONNECTOR_NAME))
                .build();

        private static Capacity unchangedCapacity() {
            return Capacity.builder()
                .provisionedCapacity(ProvisionedCapacity.builder()
                    .workerCount(UNCHANGED_WORKER_COUNT)
                    .mcuCount(MCU_COUNT)
                    .build())
                .build();
        }

        private static Capacity updatedCapacityOnlyProvisionedCapacity() {
            return Capacity.builder()
                .provisionedCapacity(ProvisionedCapacity.builder()
                    .workerCount(WORKER_COUNT)
                    .mcuCount(MCU_COUNT)
                    .build())
                .build();
        }

        private static Capacity capacityOnlyAutoscaling() {
            return Capacity.builder()
                .autoScaling(AutoScaling.builder()
                    .minWorkerCount(MIN_WORKER_COUNT)
                    .maxWorkerCount(MAX_WORKER_COUNT)
                    .scaleInPolicy(ScaleInPolicy.builder()
                        .cpuUtilizationPercentage(SCALE_IN_UTIL_PERCENT)
                        .build())
                    .scaleOutPolicy(ScaleOutPolicy.builder()
                        .cpuUtilizationPercentage(SCALE_OUT_UTIL_PERCENT)
                        .build())
                    .mcuCount(MCU_COUNT)
                    .build())
                .build();
        }

        private static CapacityUpdate capacityUpdateOnlyProvisionedCapacity() {
            return CapacityUpdate.builder()
                .provisionedCapacity(ProvisionedCapacityUpdate.builder()
                    .workerCount(WORKER_COUNT)
                    .mcuCount(MCU_COUNT)
                    .build())
                .build();
        }

        private static CapacityDescription capacityDescriptionOnlyProvisionedCapacity() {
            return CapacityDescription.builder()
                .provisionedCapacity(ProvisionedCapacityDescription.builder()
                    .workerCount(WORKER_COUNT)
                    .mcuCount(MCU_COUNT)
                    .build())
                .build();
        }

        private static CapacityDescription unchangedCapacityDescription() {
            return CapacityDescription.builder()
                .provisionedCapacity(ProvisionedCapacityDescription.builder()
                    .workerCount(UNCHANGED_WORKER_COUNT)
                    .mcuCount(MCU_COUNT)
                    .build())
                .build();
        }

        private static ResourceHandlerRequest<ResourceModel> resourceHandlerRequest() {
            return ResourceHandlerRequest
                .<ResourceModel>builder()
                .desiredResourceState(resourceModelWithCapacity(updatedCapacityOnlyProvisionedCapacity()))
                .build();
        }

        private static UpdateConnectorResponse updateConnectorResponse() {
            return UpdateConnectorResponse.builder()
                .connectorArn(CONNECTOR_ARN)
                .connectorState(ConnectorState.UPDATING)
                .build();
        }

        private static DescribeConnectorRequest describeConnectorRequest() {
            return DescribeConnectorRequest.builder()
                .connectorArn(CONNECTOR_ARN)
                .build();
        }

        private static final ProgressEvent<ResourceModel, CallbackContext> describeResponse(
            final ResourceModel resourceModel){

            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                .resourceModel(resourceModel)
                .status(OperationStatus.SUCCESS)
                .build();
        }

        private static ResourceModel resourceModelWithCapacity(final Capacity capacity) {
            return resourceModel(CONNECTOR_NAME, capacity, KAFKA_CLUSTER);
        }

        private static ResourceModel resourceModelWithName(final String name) {
            return resourceModel(name, unchangedCapacity(), KAFKA_CLUSTER);
        }

        private static ResourceModel resourceModel(final String name, final Capacity capacity,
            final KafkaCluster kafkaCluster) {
            return ResourceModel.builder()
                .connectorArn(CONNECTOR_ARN)
                .connectorName(name)
                .capacity(capacity)
                .connectorConfiguration(CONNECTOR_CONFIGURATION)
                .kafkaConnectVersion(KAFKA_CONNECT_VERSION)
                .kafkaCluster(kafkaCluster)
                .kafkaClusterClientAuthentication(KAFKA_CLUSTER_CLIENT_AUTHENTICATION)
                .kafkaClusterEncryptionInTransit(KAFKA_CLUSTER_ENCRYPTION_IN_TRANSIT)
                .plugins(new HashSet<>(PLUGINS))
                .serviceExecutionRoleArn(SERVICE_EXECUTION_ROLE_ARN)
                .build();
        }

        private static UpdateConnectorRequest updateConnectorRequest(final CapacityUpdate capacityUpdate) {
            return UpdateConnectorRequest.builder()
                .capacity(capacityUpdate)
                .connectorArn(CONNECTOR_ARN)
                .currentVersion(UNCHANGED_CURRENT_VERSION)
                .build();
        }

        private static DescribeConnectorResponse unchangedDescribeConnectorResponse(
            final CapacityDescription capacity,
            final ConnectorState connectorState) {

            return describeConnectorResponse(capacity, connectorState, UNCHANGED_CURRENT_VERSION);
        }

        private static DescribeConnectorResponse updatedDescribeConnectorResponse(
            final CapacityDescription capacity,
            final ConnectorState connectorState) {

            return describeConnectorResponse(capacity, connectorState, UPDATED_CURRENT_VERSION);
        }

        private static DescribeConnectorResponse describeConnectorResponse(
            final CapacityDescription capacity,
            final ConnectorState connectorState,
            final String version) {

            return DescribeConnectorResponse.builder()
                .capacity(capacity)
                .connectorState(connectorState)
                .currentVersion(version)
                .connectorArn(CONNECTOR_ARN)
                .connectorConfiguration(CONNECTOR_CONFIGURATION)
                .kafkaConnectVersion(KAFKA_CONNECT_VERSION)
                .kafkaCluster(KAFKA_CLUSTER_DESCRIPTION)
                .kafkaClusterClientAuthentication(KAFKA_CLUSTER_AUTH_DESCRIPTION)
                .kafkaClusterEncryptionInTransit(KAFKA_CLUSTER_IN_TRANSIT_DESCRIPTION)
                .plugins(PLUGIN_DESCRIPTIONS)
                .serviceExecutionRoleArn(SERVICE_EXECUTION_ROLE_ARN)
                .build();
        }
    }
}
