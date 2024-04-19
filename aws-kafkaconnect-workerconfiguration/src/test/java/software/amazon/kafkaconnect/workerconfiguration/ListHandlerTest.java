package software.amazon.kafkaconnect.workerconfiguration;

import org.junit.jupiter.api.AfterEach;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.kafkaconnect.KafkaConnectClient;
import software.amazon.awssdk.services.kafkaconnect.model.ListWorkerConfigurationsRequest;
import software.amazon.awssdk.services.kafkaconnect.model.ListWorkerConfigurationsResponse;
import software.amazon.awssdk.services.kafkaconnect.model.WorkerConfigurationSummary;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.proxy.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ListHandlerTest extends AbstractTestBase {

    @Mock
    private KafkaConnectClient kafkaConnectClient;

    @Mock
    private ExceptionTranslator exceptionTranslator;

    @Mock
    private Translator translator;

    private AmazonWebServicesClientProxy proxy;

    private ProxyClient<KafkaConnectClient> proxyClient;

    private ListHandler handler;

    @BeforeEach
    public void setup() {
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS,
            () -> Duration.ofSeconds(600).toMillis());
        proxyClient = proxyStub(proxy, kafkaConnectClient);
        handler = new ListHandler(exceptionTranslator, translator);
    }

    @AfterEach
    public void tear_down() {
        verifyNoMoreInteractions(kafkaConnectClient, exceptionTranslator, translator);
    }

    @Test
    public void handleRequest_success() {
        when(translator.translateToListRequest(null))
            .thenReturn(TestData.LIST_WORKER_CONFIGURATIONS_REQUEST_NEXT_TOKEN_1);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.LIST_WORKER_CONFIGURATIONS_REQUEST_NEXT_TOKEN_1,
            kafkaConnectClient::listWorkerConfigurations)).thenReturn(TestData.LIST_WORKER_CONFIGURATIONS_RESPONSE);
        when(translator.translateFromListResponse(TestData.LIST_WORKER_CONFIGURATIONS_RESPONSE))
            .thenReturn(TestData.WORKER_CONFIGURATIONS_MODELS);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler
            .handleRequest(proxy, TestData.RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, logger);

        assertThat(response).isEqualTo(TestData.SUCCESS_RESPONSE);
    }

    @Test
    public void handleRequest_success_null_next_token() {
        when(translator.translateToListRequest(TestData.NEXT_TOKEN))
            .thenReturn(TestData.LIST_WORKER_CONFIGURATIONS_REQUEST_NEXT_TOKEN_2);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.LIST_WORKER_CONFIGURATIONS_REQUEST_NEXT_TOKEN_2,
            kafkaConnectClient::listWorkerConfigurations))
                .thenReturn(TestData.LIST_WORKER_CONFIGURATIONS_RESPONSE_NULL_NEXT_TOKEN);
        when(translator.translateFromListResponse(TestData.LIST_WORKER_CONFIGURATIONS_RESPONSE_NULL_NEXT_TOKEN))
            .thenReturn(TestData.WORKER_CONFIGURATIONS_MODELS_2);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler
            .handleRequest(proxy, TestData.RESOURCE_HANDLER_REQUEST_2, new CallbackContext(), proxyClient, logger);

        assertThat(response).isEqualTo(TestData.SUCCESS_RESPONSE_NULL_NEXT_TOKEN);
    }

    @Test
    public void handleRequest_throwsException_whenListWorkerConfigurationsFails() {
        final AwsServiceException serviceException = AwsServiceException.builder().build();
        final CfnGeneralServiceException cfnException = new CfnGeneralServiceException(serviceException);
        when(translator.translateToListRequest(null))
            .thenReturn(TestData.LIST_WORKER_CONFIGURATIONS_REQUEST_NEXT_TOKEN_1);
        when(proxyClient
            .injectCredentialsAndInvokeV2(TestData.LIST_WORKER_CONFIGURATIONS_REQUEST_NEXT_TOKEN_1,
                kafkaConnectClient::listWorkerConfigurations))
                    .thenThrow(serviceException);
        when(exceptionTranslator.translateToCfnException(serviceException, TestData.AWS_ACCOUNT_ID))
            .thenReturn(cfnException);

        final CfnGeneralServiceException exception =
            assertThrows(
                CfnGeneralServiceException.class, () -> handler.handleRequest(
                    proxy, TestData.RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, logger));

        assertThat(exception).isEqualTo(cfnException);
    }

    private static class TestData {
        private static final String NEXT_TOKEN = "1234abcd";
        private static final String AWS_ACCOUNT_ID = "1111111111";
        private static final String WORKER_CONFIGURATION_ARN_1 =
            "arn:aws:kafkaconnect:us-east-1:1111111111:worker-configuration/unit-test-worker-configuration-1";
        private static final String WORKER_CONFIGURATION_ARN_2 =
            "arn:aws:kafkaconnect:us-east-1:1111111111:worker-configuration/unit-test-worker-configuration-2";
        private static final String WORKER_CONFIGURATION_ARN_3 =
            "arn:aws:kafkaconnect:us-east-1:1111111111:worker-configuration/unit-test-worker-configuration-3";
        private static final String WORKER_CONFIGURATION_ARN_4 =
            "arn:aws:kafkaconnect:us-east-1:1111111111:worker-configuration/unit-test-worker-configuration-4";
        private static final ListWorkerConfigurationsRequest LIST_WORKER_CONFIGURATIONS_REQUEST_NEXT_TOKEN_1 =
            ListWorkerConfigurationsRequest
                .builder()
                .nextToken(null)
                .build();

        private static final ListWorkerConfigurationsRequest LIST_WORKER_CONFIGURATIONS_REQUEST_NEXT_TOKEN_2 =
            ListWorkerConfigurationsRequest
                .builder()
                .nextToken(NEXT_TOKEN)
                .build();
        private static final ResourceModel MODEL = ResourceModel.builder().build();
        private static final ResourceHandlerRequest<ResourceModel> RESOURCE_HANDLER_REQUEST =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(MODEL)
                .nextToken(null)
                .awsAccountId(AWS_ACCOUNT_ID)
                .build();

        private static final ResourceHandlerRequest<ResourceModel> RESOURCE_HANDLER_REQUEST_2 =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(MODEL)
                .nextToken(NEXT_TOKEN)
                .awsAccountId(AWS_ACCOUNT_ID)
                .build();

        public static final List<ResourceModel> WORKER_CONFIGURATIONS_MODELS = Arrays.asList(
            testResourceModel(WORKER_CONFIGURATION_ARN_1),
            testResourceModel(WORKER_CONFIGURATION_ARN_2));

        private static final List<WorkerConfigurationSummary> WORKER_CONFIGURATION_SUMMARIES = Arrays.asList(
            testWorkerConfigurationSummary(WORKER_CONFIGURATION_ARN_1),
            testWorkerConfigurationSummary(WORKER_CONFIGURATION_ARN_2));

        public static final List<ResourceModel> WORKER_CONFIGURATIONS_MODELS_2 = Arrays.asList(
            testResourceModel(WORKER_CONFIGURATION_ARN_3),
            testResourceModel(WORKER_CONFIGURATION_ARN_4));

        private static final List<WorkerConfigurationSummary> WORKER_CONFIGURATION_SUMMARIES_2 = Arrays.asList(
            testWorkerConfigurationSummary(WORKER_CONFIGURATION_ARN_3),
            testWorkerConfigurationSummary(WORKER_CONFIGURATION_ARN_4));

        private static final ProgressEvent<ResourceModel, CallbackContext> SUCCESS_RESPONSE =
            ProgressEvent.<ResourceModel, CallbackContext>builder()
                .resourceModels(WORKER_CONFIGURATIONS_MODELS)
                .nextToken(NEXT_TOKEN)
                .status(OperationStatus.SUCCESS)
                .build();

        private static final ProgressEvent<ResourceModel, CallbackContext> SUCCESS_RESPONSE_NULL_NEXT_TOKEN =
            ProgressEvent.<ResourceModel, CallbackContext>builder()
                .resourceModels(WORKER_CONFIGURATIONS_MODELS_2)
                .nextToken(null)
                .status(OperationStatus.SUCCESS)
                .build();

        private static final ListWorkerConfigurationsResponse LIST_WORKER_CONFIGURATIONS_RESPONSE =
            ListWorkerConfigurationsResponse
                .builder()
                .nextToken(TestData.NEXT_TOKEN)
                .workerConfigurations(TestData.WORKER_CONFIGURATION_SUMMARIES)
                .build();

        private static final ListWorkerConfigurationsResponse LIST_WORKER_CONFIGURATIONS_RESPONSE_NULL_NEXT_TOKEN =
            ListWorkerConfigurationsResponse
                .builder()
                .nextToken(null)
                .workerConfigurations(TestData.WORKER_CONFIGURATION_SUMMARIES_2)
                .build();

        private static WorkerConfigurationSummary testWorkerConfigurationSummary(final String arn) {
            return WorkerConfigurationSummary
                .builder()
                .workerConfigurationArn(arn)
                .build();
        }

        private static ResourceModel testResourceModel(final String arn) {
            return ResourceModel
                .builder()
                .workerConfigurationArn(arn)
                .build();
        }
    }
}
