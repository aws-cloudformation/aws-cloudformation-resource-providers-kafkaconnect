package software.amazon.kafkaconnect.workerconfiguration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.kafkaconnect.KafkaConnectClient;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeWorkerConfigurationRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeWorkerConfigurationResponse;
import software.amazon.awssdk.services.kafkaconnect.model.NotFoundException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.*;

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
    public void runDescribeWorkerConfiguration_success() {
        when(proxyClient.injectCredentialsAndInvokeV2(TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST,
            kafkaConnectClient::describeWorkerConfiguration))
                .thenReturn(TestData.DESCRIBE_WORKER_CONFIGURATION_RESPONSE);

        final DescribeWorkerConfigurationResponse describeWorkerConfigurationResponse =
            stubHandler.runDescribeWorkerConfiguration(
                TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST, proxyClient, TestData.FAILURE_MESSAGE_PATTERN);

        assertThat(describeWorkerConfigurationResponse).isEqualTo(TestData.DESCRIBE_WORKER_CONFIGURATION_RESPONSE);
    }

    @Test
    public void runDescribeWorkerConfiguration_throwsCfnGeneralServiceException_whenDescribeFails() {
        when(proxyClient.injectCredentialsAndInvokeV2(TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST,
            kafkaConnectClient::describeWorkerConfiguration)).thenThrow(AwsServiceException.builder()
                .message(TestData.EXCEPTION_MESSAGE).build());

        final CfnGeneralServiceException serviceException = assertThrows(CfnGeneralServiceException.class,
            () -> stubHandler.runDescribeWorkerConfiguration(TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST,
                proxyClient,
                TestData.FAILURE_MESSAGE_PATTERN));

        assertThat(serviceException.getMessage()).isEqualTo(String.format("Error occurred during operation '" +
            TestData.FAILURE_MESSAGE_PATTERN + "'.", ResourceModel.TYPE_NAME, TestData.EXCEPTION_MESSAGE));
    }

    @Test
    public void runDescribeWorkerConfigurationWithNotFoundCatch_success() {
        when(proxyClient.injectCredentialsAndInvokeV2(TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST,
            kafkaConnectClient::describeWorkerConfiguration))
                .thenReturn(TestData.DESCRIBE_WORKER_CONFIGURATION_RESPONSE);

        final DescribeWorkerConfigurationResponse describeWorkerConfigurationResponse =
            stubHandler.runDescribeWorkerConfigurationWithNotFoundCatch(
                TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST, proxyClient, TestData.FAILURE_MESSAGE_PATTERN,
                exceptionTranslator);

        assertThat(describeWorkerConfigurationResponse).isEqualTo(TestData.DESCRIBE_WORKER_CONFIGURATION_RESPONSE);
    }

    @Test
    public void runDescribeWorkerConfigurationWithNotFoundCatch_throwsCfnNotFoundException_whenWorkerConfigurationDoesNotExist() {
        final NotFoundException exception = NotFoundException.builder()
            .message(TestData.EXCEPTION_MESSAGE).build();
        final CfnNotFoundException cfnException = new CfnNotFoundException(ResourceModel.TYPE_NAME,
            TestData.WORKER_CONFIGURATION_ARN, exception);
        when(proxyClient.injectCredentialsAndInvokeV2(TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST,
            kafkaConnectClient::describeWorkerConfiguration)).thenThrow(exception);
        when(exceptionTranslator.translateToCfnException(exception, TestData.WORKER_CONFIGURATION_ARN))
            .thenReturn(cfnException);

        final CfnNotFoundException responseException = assertThrows(CfnNotFoundException.class,
            () -> stubHandler.runDescribeWorkerConfigurationWithNotFoundCatch(
                TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST, proxyClient,
                TestData.FAILURE_MESSAGE_PATTERN, exceptionTranslator));

        assertThat(responseException).isEqualTo(cfnException);
    }

    @Test
    public void runDescribeWorkerConfigurationWithNotFoundCatch_throwsCfnGeneralServiceException_whenDescribeFails() {
        when(proxyClient.injectCredentialsAndInvokeV2(TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST,
            kafkaConnectClient::describeWorkerConfiguration)).thenThrow(AwsServiceException.builder()
                .message(TestData.EXCEPTION_MESSAGE).build());

        final CfnGeneralServiceException serviceException = assertThrows(CfnGeneralServiceException.class,
            () -> stubHandler.runDescribeWorkerConfigurationWithNotFoundCatch(
                TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST, proxyClient,
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

        private static final String WORKER_CONFIGURATION_ARN =
            "arn:aws:kafkaconnect:us-east-1:1111111111:worker-configuration/unit-test-worker-configuration";

        private static final String FAILURE_MESSAGE_PATTERN = "%s FAILED because of %s";
        private static final String EXCEPTION_MESSAGE = "Exception Message";

        private static final DescribeWorkerConfigurationRequest DESCRIBE_WORKER_CONFIGURATION_REQUEST =
            DescribeWorkerConfigurationRequest
                .builder()
                .workerConfigurationArn(WORKER_CONFIGURATION_ARN)
                .build();

        private static final DescribeWorkerConfigurationResponse DESCRIBE_WORKER_CONFIGURATION_RESPONSE =
            DescribeWorkerConfigurationResponse
                .builder()
                .workerConfigurationArn(WORKER_CONFIGURATION_ARN)
                .build();
    }
}
