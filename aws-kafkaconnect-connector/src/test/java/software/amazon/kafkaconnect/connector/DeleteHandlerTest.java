package software.amazon.kafkaconnect.connector;

import java.time.Duration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.kafkaconnect.KafkaConnectClient;
import software.amazon.awssdk.services.kafkaconnect.model.ConnectorState;
import software.amazon.awssdk.services.kafkaconnect.model.DeleteConnectorRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DeleteConnectorResponse;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeConnectorRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeConnectorResponse;
import software.amazon.awssdk.services.kafkaconnect.model.NotFoundException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DeleteHandlerTest extends AbstractTestBase {

    @Mock
    private KafkaConnectClient kafkaConnectClient;

    @Mock
    private ExceptionTranslator exceptionTranslator;

    @Mock
    private Translator translator;

    private AmazonWebServicesClientProxy proxy;

    private ProxyClient<KafkaConnectClient> proxyClient;

    private DeleteHandler handler;

    @BeforeEach
    public void setup() {
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS,
            () -> Duration.ofSeconds(600).toMillis());
        proxyClient = proxyStub(proxy, kafkaConnectClient);
        handler = new DeleteHandler(exceptionTranslator, translator);
    }

    @AfterEach
    public void tear_down() {
        verify(kafkaConnectClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(kafkaConnectClient, exceptionTranslator, translator);
    }

    @Test
    public void handleRequest_success() {
        setupDeleteRequestMocks();
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DESCRIBE_CONNECTOR_REQUEST,
            kafkaConnectClient::describeConnector)
        )
            .thenReturn(TestData.describeConnectorResponse(ConnectorState.RUNNING))
            .thenReturn(TestData.DESCRIBE_CONNECTOR_RESPONSE)
            .thenReturn(TestData.DESCRIBE_CONNECTOR_RESPONSE)
            .thenThrow(NotFoundException.class);

        runAndVerifySuccess();
    }

    @Test
    public void handleRequest_waitForConnectorInCreatingState_success() {
        setupSuccessWithWaitingForConnector(ConnectorState.CREATING);

        runAndVerifySuccess();
    }

    @Test
    public void handleRequest_waitForConnectorInUpdatingState_success() {
        setupSuccessWithWaitingForConnector(ConnectorState.UPDATING);

        runAndVerifySuccess();
    }

    @Test
    public void handleRequest_throwsCfnNotFoundException_whenConnectorDoesNotExist() {
        final NotFoundException serviceException = NotFoundException.builder().build();
        final CfnNotFoundException cfnException = new CfnNotFoundException(serviceException);
        when(translator.translateToReadRequest(TestData.RESOURCE_MODEL))
            .thenReturn(TestData.DESCRIBE_CONNECTOR_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DESCRIBE_CONNECTOR_REQUEST,
            kafkaConnectClient::describeConnector)
        ).thenThrow(serviceException);
        when(exceptionTranslator.translateToCfnException(serviceException, TestData.CONNECTOR_ARN))
            .thenReturn(cfnException);

        final CfnNotFoundException exception = assertThrows(CfnNotFoundException.class, () -> handler
            .handleRequest(proxy, TestData.RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, logger));

        assertThat(exception).isEqualTo(cfnException);
    }

    @Test
    public void handleRequest_throwsCfnGeneralServiceException_whenIsDeletableConnectorStateFails() {
        final AwsServiceException serviceException = AwsServiceException.builder()
            .message(TestData.EXCEPTION_MESSAGE).build();
        when(translator.translateToReadRequest(TestData.RESOURCE_MODEL))
            .thenReturn(TestData.DESCRIBE_CONNECTOR_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DESCRIBE_CONNECTOR_REQUEST,
            kafkaConnectClient::describeConnector)
        ).thenThrow(serviceException);

        final CfnGeneralServiceException exception = assertThrows(CfnGeneralServiceException.class, () -> handler
            .handleRequest(proxy, TestData.RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, logger));

        assertThat(exception.getMessage()).isEqualTo(String.format(
            "Error occurred during operation 'Could not initiate deletion of %s. Failed to get state due to: %s'.",
            ResourceModel.TYPE_NAME, TestData.EXCEPTION_MESSAGE));
    }

    @Test
    public void handleRequest_throwsCfnGeneralServiceException_whenStabilizeDescribeConnectorFails() {
        final AwsServiceException serviceException = AwsServiceException.builder()
            .message(TestData.EXCEPTION_MESSAGE).build();
        setupDeleteRequestMocks();
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DESCRIBE_CONNECTOR_REQUEST,
            kafkaConnectClient::describeConnector)
        )
            .thenReturn(TestData.describeConnectorResponse(ConnectorState.RUNNING))
            .thenThrow(serviceException);

        final CfnGeneralServiceException exception = assertThrows(CfnGeneralServiceException.class,
            () -> handler.handleRequest(proxy, TestData.RESOURCE_HANDLER_REQUEST, new CallbackContext(),
                proxyClient, logger));

        assertThat(exception.getMessage()).isEqualTo("Error occurred during operation '" + ResourceModel.TYPE_NAME +
            " [" + TestData.CONNECTOR_ARN + "] deletion status couldn't be retrieved: " + TestData.EXCEPTION_MESSAGE
            + "'.");
    }

    @Test
    public void handleRequest_throwsCfnGeneralServiceException_whenConnectorEndsInFailedState() {
        setupDeleteRequestMocks();
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DESCRIBE_CONNECTOR_REQUEST,
            kafkaConnectClient::describeConnector)
        )
            .thenReturn(TestData.describeConnectorResponse(ConnectorState.RUNNING))
            .thenReturn(TestData.DESCRIBE_CONNECTOR_RESPONSE_FAILED);

        final CfnGeneralServiceException exception = assertThrows(CfnGeneralServiceException.class,
            () -> handler.handleRequest(proxy, TestData.RESOURCE_HANDLER_REQUEST, new CallbackContext(),
                proxyClient, logger));

        assertThat(exception.getMessage()).isEqualTo(String.format("Error occurred during operation '%s " +
            "[%s] failed to delete'.", ResourceModel.TYPE_NAME, TestData.CONNECTOR_ARN));
    }

    private void runAndVerifySuccess() {
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(
            proxy, TestData.RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, logger);

        assertThat(response).isEqualTo(TestData.DELETE_SUCCESS_RESPONSE);
    }

    private void setupSuccessWithWaitingForConnector(final ConnectorState connectorState) {
        setupDeleteRequestMocks();
        final DescribeConnectorResponse waitingResponse = TestData.describeConnectorResponse(connectorState);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DESCRIBE_CONNECTOR_REQUEST,
            kafkaConnectClient::describeConnector)
        )
            .thenReturn(waitingResponse)
            .thenReturn(waitingResponse)
            .thenReturn(TestData.describeConnectorResponse(ConnectorState.RUNNING))
            .thenReturn(TestData.DESCRIBE_CONNECTOR_RESPONSE)
            .thenReturn(TestData.DESCRIBE_CONNECTOR_RESPONSE)
            .thenThrow(NotFoundException.class);
    }

    private void setupDeleteRequestMocks() {
        when(translator.translateToDeleteRequest(TestData.RESOURCE_MODEL))
            .thenReturn(TestData.DELETE_CONNECTOR_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DELETE_CONNECTOR_REQUEST,
            kafkaConnectClient::deleteConnector)
        ).thenReturn(TestData.DELETE_CONNECTOR_RESPONSE);
        when(translator.translateToReadRequest(TestData.RESOURCE_MODEL))
            .thenReturn(TestData.DESCRIBE_CONNECTOR_REQUEST);
    }

    private static class TestData {
        private static final String CONNECTOR_ARN =
            "arn:aws:kafkaconnect:us-east-1:123456789:connector/unit-test-connector";
        private static final String EXCEPTION_MESSAGE = "Exception Message";

        private static final DeleteConnectorRequest DELETE_CONNECTOR_REQUEST = DeleteConnectorRequest
            .builder()
            .connectorArn(CONNECTOR_ARN)
            .build();

        private static final DeleteConnectorResponse DELETE_CONNECTOR_RESPONSE = DeleteConnectorResponse
            .builder()
            .connectorArn(CONNECTOR_ARN)
            .build();

        private static final DescribeConnectorRequest DESCRIBE_CONNECTOR_REQUEST = DescribeConnectorRequest
            .builder()
            .connectorArn(CONNECTOR_ARN)
            .build();

        private static final ResourceModel RESOURCE_MODEL = ResourceModel
            .builder()
            .connectorArn(CONNECTOR_ARN)
            .build();

        private static final ResourceHandlerRequest<ResourceModel> RESOURCE_HANDLER_REQUEST = ResourceHandlerRequest
            .<ResourceModel>builder()
            .desiredResourceState(RESOURCE_MODEL)
            .build();

        private static final DescribeConnectorResponse DESCRIBE_CONNECTOR_RESPONSE = DescribeConnectorResponse
            .builder().build();

        private static final DescribeConnectorResponse DESCRIBE_CONNECTOR_RESPONSE_FAILED = DescribeConnectorResponse
            .builder().connectorState(ConnectorState.FAILED).build();

        private static final ProgressEvent<ResourceModel, CallbackContext> DELETE_SUCCESS_RESPONSE =
            ProgressEvent.<ResourceModel, CallbackContext>builder()
                .status(OperationStatus.SUCCESS)
                .build();

        private static DescribeConnectorResponse describeConnectorResponse(final ConnectorState state) {
            return DescribeConnectorResponse.builder().connectorState(state.toString()).build();
        }
    }
}
