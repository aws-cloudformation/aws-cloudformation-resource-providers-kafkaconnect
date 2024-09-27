package software.amazon.kafkaconnect.workerconfiguration;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.stream.Stream;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.kafkaconnect.KafkaConnectClient;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeWorkerConfigurationRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeWorkerConfigurationResponse;
import software.amazon.awssdk.services.kafkaconnect.model.WorkerConfigurationState;
import software.amazon.awssdk.services.kafkaconnect.model.DeleteWorkerConfigurationRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DeleteWorkerConfigurationResponse;
import software.amazon.awssdk.services.kafkaconnect.model.NotFoundException;
import software.amazon.awssdk.services.kafkaconnect.model.BadRequestException;
import software.amazon.awssdk.services.kafkaconnect.model.InternalServerErrorException;
import software.amazon.awssdk.services.kafkaconnect.model.WorkerConfigurationRevisionDescription;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.exceptions.CfnNotStabilizedException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.times;

@ExtendWith(MockitoExtension.class)
public class DeleteHandlerTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<KafkaConnectClient> proxyClient;

    @Mock
    private KafkaConnectClient kafkaConnectClient;

    @Mock
    private ExceptionTranslator exceptionTranslator;

    @Mock
    private Translator translator;

    private DeleteHandler handler;

    private static Stream<Arguments> stabilizeKafkaConnectErrorToCfnError() {
        return Stream.of(
            arguments(BadRequestException.builder().build()),
            arguments(InternalServerErrorException.builder().build()),
            arguments(AwsServiceException.builder().build()));
    }

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
    public void handleRequest_SimpleSuccess() {
        final DescribeWorkerConfigurationResponse describeWorkerConfigurationResponse = TestData.describeResponse();

        when(translator.translateToReadRequest(TestData.RESOURCE_MODEL))
            .thenReturn(TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST, kafkaConnectClient::describeWorkerConfiguration))
                .thenReturn(describeWorkerConfigurationResponse).thenReturn(describeWorkerConfigurationResponse)
                .thenThrow(NotFoundException.class);
        when(proxyClient.client().deleteWorkerConfiguration(any(DeleteWorkerConfigurationRequest.class)))
            .thenReturn(TestData.DELETE_WORKER_CONFIGURATION_RESPONSE);
        when(translator.translateToDeleteRequest(TestData.RESOURCE_MODEL))
            .thenReturn(TestData.DELETE_WORKER_CONFIGURATION_REQUEST);
        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(proxy, TestData.RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(1)).deleteWorkerConfiguration(any(DeleteWorkerConfigurationRequest.class));
        verify(proxyClient.client(), times(3))
            .describeWorkerConfiguration(any(DescribeWorkerConfigurationRequest.class));
    }

    @Test
    public void handleStabilize_UnexpectedStatus() {
        final DescribeWorkerConfigurationResponse describeWorkerConfigurationResponse = TestData.describeResponse();

        when(translator.translateToReadRequest(TestData.RESOURCE_MODEL))
            .thenReturn(TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST, kafkaConnectClient::describeWorkerConfiguration))
                .thenReturn(describeWorkerConfigurationResponse).thenReturn(describeWorkerConfigurationResponse
                    .toBuilder().workerConfigurationState(WorkerConfigurationState.UNKNOWN_TO_SDK_VERSION).build());
        when(proxyClient.client().deleteWorkerConfiguration(any(DeleteWorkerConfigurationRequest.class)))
            .thenReturn(TestData.DELETE_WORKER_CONFIGURATION_RESPONSE);
        when(translator.translateToDeleteRequest(TestData.RESOURCE_MODEL))
            .thenReturn(TestData.DELETE_WORKER_CONFIGURATION_REQUEST);

        assertThrows(CfnNotStabilizedException.class,
            () -> handler.handleRequest(proxy, TestData.RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient,
                logger));
        verify(proxyClient.client(), times(1)).deleteWorkerConfiguration(any(DeleteWorkerConfigurationRequest.class));
        verify(proxyClient.client(), times(2))
            .describeWorkerConfiguration(any(DescribeWorkerConfigurationRequest.class));
    }

    @Test
    public void handleStabilize_BadRequest_InvalidParameter() {
        final BadRequestException serviceException = BadRequestException.builder().build();
        final CfnInvalidRequestException cfnException = new CfnInvalidRequestException(serviceException);

        when(translator.translateToReadRequest(TestData.RESOURCE_MODEL))
            .thenReturn(TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST, kafkaConnectClient::describeWorkerConfiguration))
                .thenThrow(serviceException);
        when(exceptionTranslator.translateToCfnException(serviceException, TestData.WORKER_CONFIGURATION_ARN))
            .thenReturn(cfnException);

        assertThrows(CfnInvalidRequestException.class,
            () -> handler.handleRequest(proxy, TestData.RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient,
                logger));

        verify(proxyClient.client(), times(0)).deleteWorkerConfiguration(any(DeleteWorkerConfigurationRequest.class));
        verify(proxyClient.client(), times(1))
            .describeWorkerConfiguration(any(DescribeWorkerConfigurationRequest.class));
    }

    @Test
    public void handleDelete_ResourceNotFound_AlreadyDeletedFailure() {
        final DescribeWorkerConfigurationResponse describeWorkerConfigurationResponse = TestData.describeResponse();
        final NotFoundException serviceException = NotFoundException.builder().build();
        final CfnNotFoundException cfnException = new CfnNotFoundException(serviceException);

        when(translator.translateToReadRequest(TestData.RESOURCE_MODEL))
            .thenReturn(TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST, kafkaConnectClient::describeWorkerConfiguration))
                .thenReturn(describeWorkerConfigurationResponse);
        when(proxyClient.client().deleteWorkerConfiguration(any(DeleteWorkerConfigurationRequest.class)))
            .thenThrow(serviceException);
        when(exceptionTranslator.translateToCfnException(serviceException, TestData.WORKER_CONFIGURATION_ARN))
            .thenReturn(cfnException);
        when(translator.translateToDeleteRequest(TestData.RESOURCE_MODEL))
            .thenReturn(TestData.DELETE_WORKER_CONFIGURATION_REQUEST);

        assertThrows(CfnNotFoundException.class,
            () -> handler.handleRequest(proxy, TestData.RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient,
                logger));

        verify(proxyClient.client(), times(1)).deleteWorkerConfiguration(any(DeleteWorkerConfigurationRequest.class));
        verify(kafkaConnectClient, atLeastOnce()).serviceName();
    }

    @ParameterizedTest
    @MethodSource("stabilizeKafkaConnectErrorToCfnError")
    public void handleStabilize_Exception(
        AwsServiceException kafkaConnectException) {

        final DescribeWorkerConfigurationResponse describeWorkerConfigurationResponse = TestData.describeResponse();

        when(translator.translateToReadRequest(TestData.RESOURCE_MODEL))
            .thenReturn(TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST, kafkaConnectClient::describeWorkerConfiguration))
                .thenReturn(describeWorkerConfigurationResponse).thenThrow(kafkaConnectException);
        when(proxyClient.client().deleteWorkerConfiguration(any(DeleteWorkerConfigurationRequest.class)))
            .thenReturn(TestData.DELETE_WORKER_CONFIGURATION_RESPONSE);
        when(translator.translateToDeleteRequest(TestData.RESOURCE_MODEL))
            .thenReturn(TestData.DELETE_WORKER_CONFIGURATION_REQUEST);

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(proxy, TestData.RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();

        verify(proxyClient.client(), times(1)).deleteWorkerConfiguration(any(DeleteWorkerConfigurationRequest.class));
        verify(proxyClient.client(), times(2))
            .describeWorkerConfiguration(any(DescribeWorkerConfigurationRequest.class));
        verify(exceptionTranslator, times(1)).translateToCfnException(any(AwsServiceException.class),
            eq(TestData.WORKER_CONFIGURATION_ARN));
    }

    private static class TestData {
        private static final String WORKER_CONFIGURATION_ARN =
            "arn:aws:kafkaconnect:us-east-1:1111111111:worker-configuration/unit-test-worker-configuration";

        private static final String WORKER_CONFIGURATION_NAME = "unit-test-worker-configuration";

        private static final String WORKER_CONFIGURATION_DESCRIPTION = "Unit testing worker configuration description";

        private static final long WORKER_CONFIGURATION_REVISION = 1L;

        private static final String WORKER_CONFIGURATION_PROPERTIES_FILE_CONTENT = "propertiesFileContent";

        private static final Instant WORKER_CONFIGURATION_CREATION_TIME =
            OffsetDateTime.parse("2021-03-04T14:03:40.818Z").toInstant();
        private static final DescribeWorkerConfigurationRequest DESCRIBE_WORKER_CONFIGURATION_REQUEST =
            DescribeWorkerConfigurationRequest.builder()
                .workerConfigurationArn(WORKER_CONFIGURATION_ARN)
                .build();

        private static final ResourceModel RESOURCE_MODEL = ResourceModel.builder()
            .workerConfigurationArn(TestData.WORKER_CONFIGURATION_ARN)
            .name(TestData.WORKER_CONFIGURATION_NAME)
            .description(TestData.WORKER_CONFIGURATION_DESCRIPTION)
            .revision(TestData.WORKER_CONFIGURATION_REVISION)
            .propertiesFileContent(TestData.WORKER_CONFIGURATION_PROPERTIES_FILE_CONTENT)
            .build();

        private static WorkerConfigurationRevisionDescription workerConfigurationRevisionDescription() {
            return WorkerConfigurationRevisionDescription.builder()
                .revision(WORKER_CONFIGURATION_REVISION)
                .description(WORKER_CONFIGURATION_DESCRIPTION)
                .propertiesFileContent(WORKER_CONFIGURATION_PROPERTIES_FILE_CONTENT)
                .build();
        }

        private static DescribeWorkerConfigurationResponse describeResponse() {
            return DescribeWorkerConfigurationResponse
                .builder()
                .workerConfigurationArn(WORKER_CONFIGURATION_ARN)
                .creationTime(WORKER_CONFIGURATION_CREATION_TIME)
                .name(WORKER_CONFIGURATION_NAME)
                .description(WORKER_CONFIGURATION_DESCRIPTION)
                .latestRevision(workerConfigurationRevisionDescription())
                .workerConfigurationState(WorkerConfigurationState.DELETING)
                .build();
        }

        private static final ResourceHandlerRequest<ResourceModel> RESOURCE_HANDLER_REQUEST =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL)
                .build();

        private static final DeleteWorkerConfigurationRequest DELETE_WORKER_CONFIGURATION_REQUEST =
            DeleteWorkerConfigurationRequest.builder().workerConfigurationArn(WORKER_CONFIGURATION_ARN).build();

        private static final DeleteWorkerConfigurationResponse DELETE_WORKER_CONFIGURATION_RESPONSE =
            DeleteWorkerConfigurationResponse.builder().workerConfigurationArn(WORKER_CONFIGURATION_ARN).build();
    }
}
