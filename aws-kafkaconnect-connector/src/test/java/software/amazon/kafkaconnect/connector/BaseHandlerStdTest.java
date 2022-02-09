package software.amazon.kafkaconnect.connector;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.kafkaconnect.KafkaConnectClient;
import software.amazon.awssdk.services.kafkaconnect.model.ConnectorState;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeConnectorRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeConnectorResponse;
import software.amazon.awssdk.services.kafkaconnect.model.NotFoundException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class BaseHandlerStdTest extends AbstractTestBase {

    @Mock
    private KafkaConnectClient kafkaConnectClient;

    @Mock
    private ExceptionTranslator exceptionTranslator;

    private ProxyClient<KafkaConnectClient> proxyClient;

    private StubHandler stubHandler;

    @BeforeEach
    public void setup() {
        final AmazonWebServicesClientProxy proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS,
            () -> Duration.ofSeconds(600).toMillis());
        proxyClient = proxyStub(proxy, kafkaConnectClient);
        stubHandler = new StubHandler();
    }

    @AfterEach
    public void tear_down() {
        verifyNoMoreInteractions(kafkaConnectClient);
    }

    @Test
    public void runDescribeConnector_success() {
        when(proxyClient.injectCredentialsAndInvokeV2(TestData.DESCRIBE_CONNECTOR_REQUEST,
            kafkaConnectClient::describeConnector)).thenReturn(TestData.DESCRIBE_CONNECTOR_RESPONSE);

        final DescribeConnectorResponse describeConnectorResponse = stubHandler.runDescribeConnector(
            TestData.DESCRIBE_CONNECTOR_REQUEST, proxyClient, TestData.FAILURE_MESSAGE_PATTERN);

        assertThat(describeConnectorResponse).isEqualTo(TestData.DESCRIBE_CONNECTOR_RESPONSE);
    }

    @Test
    public void runDescribeConnector_throwsCfnGeneralServiceException_whenDescribeFails() {
        when(proxyClient.injectCredentialsAndInvokeV2(TestData.DESCRIBE_CONNECTOR_REQUEST,
            kafkaConnectClient::describeConnector)).thenThrow(AwsServiceException.builder()
                .message(TestData.EXCEPTION_MESSAGE).build());

        final CfnGeneralServiceException serviceException = assertThrows(CfnGeneralServiceException.class,
            () -> stubHandler.runDescribeConnector(TestData.DESCRIBE_CONNECTOR_REQUEST, proxyClient,
                TestData.FAILURE_MESSAGE_PATTERN));

        assertThat(serviceException.getMessage()).isEqualTo(String.format("Error occurred during operation '" +
            TestData.FAILURE_MESSAGE_PATTERN + "'.", ResourceModel.TYPE_NAME, TestData.EXCEPTION_MESSAGE));
    }

    @Test
    public void runDescribeConnectorWithNotFoundCatch_success() {
        when(proxyClient.injectCredentialsAndInvokeV2(TestData.DESCRIBE_CONNECTOR_REQUEST,
            kafkaConnectClient::describeConnector)).thenReturn(TestData.DESCRIBE_CONNECTOR_RESPONSE);

        final DescribeConnectorResponse describeConnectorResponse = stubHandler.runDescribeConnectorWithNotFoundCatch(
            TestData.DESCRIBE_CONNECTOR_REQUEST, proxyClient, TestData.FAILURE_MESSAGE_PATTERN, exceptionTranslator);

        assertThat(describeConnectorResponse).isEqualTo(TestData.DESCRIBE_CONNECTOR_RESPONSE);
    }

    @Test
    public void runDescribeConnectorWithNotFoundCatch_throwsCfnNotFoundException_whenConnectorDoesNotExist() {
        final NotFoundException exception = NotFoundException.builder()
            .message(TestData.EXCEPTION_MESSAGE).build();
        final CfnNotFoundException cfnException = new CfnNotFoundException(ResourceModel.TYPE_NAME,
            TestData.CONNECTOR_ARN, exception);
        when(proxyClient.injectCredentialsAndInvokeV2(TestData.DESCRIBE_CONNECTOR_REQUEST,
            kafkaConnectClient::describeConnector)).thenThrow(exception);
        when(exceptionTranslator.translateToCfnException(exception, TestData.CONNECTOR_ARN))
            .thenReturn(cfnException);

        final CfnNotFoundException responseException = assertThrows(CfnNotFoundException.class,
            () -> stubHandler.runDescribeConnectorWithNotFoundCatch(TestData.DESCRIBE_CONNECTOR_REQUEST, proxyClient,
                TestData.FAILURE_MESSAGE_PATTERN, exceptionTranslator));

        assertThat(responseException).isEqualTo(cfnException);
    }

    @Test
    public void runDescribeConnectorWithNotFoundCatch_throwsCfnGeneralServiceException_whenDescribeFails() {
        when(proxyClient.injectCredentialsAndInvokeV2(TestData.DESCRIBE_CONNECTOR_REQUEST,
            kafkaConnectClient::describeConnector)).thenThrow(AwsServiceException.builder()
            .message(TestData.EXCEPTION_MESSAGE).build());

        final CfnGeneralServiceException serviceException = assertThrows(CfnGeneralServiceException.class,
            () -> stubHandler.runDescribeConnectorWithNotFoundCatch(TestData.DESCRIBE_CONNECTOR_REQUEST, proxyClient,
                TestData.FAILURE_MESSAGE_PATTERN, exceptionTranslator));

        assertThat(serviceException.getMessage()).isEqualTo(String.format("Error occurred during operation '" +
            TestData.FAILURE_MESSAGE_PATTERN + "'.", ResourceModel.TYPE_NAME, TestData.EXCEPTION_MESSAGE));
    }

    private class StubHandler extends BaseHandlerStd {
        @Override
        protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<KafkaConnectClient> proxyClient,
            final Logger logger) {

            return super.handleRequest(proxy, request, callbackContext, logger);
        }
    }

    private static class TestData {
        private static final String CONNECTOR_ARN =
            "arn:aws:kafkaconnect:us-east-1:123456789:connector/unit-test-connector";
        private static final ConnectorState CONNECTOR_STATE = ConnectorState.RUNNING;
        private static final String FAILURE_MESSAGE_PATTERN = "%s FAILED because of %s";
        private static final String EXCEPTION_MESSAGE = "Exception Message";

        private static final DescribeConnectorRequest  DESCRIBE_CONNECTOR_REQUEST = DescribeConnectorRequest
            .builder()
            .connectorArn(CONNECTOR_ARN)
            .build();

        private static final DescribeConnectorResponse DESCRIBE_CONNECTOR_RESPONSE = DescribeConnectorResponse
            .builder()
            .connectorState(CONNECTOR_STATE)
            .connectorArn(CONNECTOR_ARN)
            .build();
    }
}
