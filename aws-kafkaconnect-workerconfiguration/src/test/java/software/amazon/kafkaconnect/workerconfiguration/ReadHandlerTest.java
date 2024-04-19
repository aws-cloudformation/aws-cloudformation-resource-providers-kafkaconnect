package software.amazon.kafkaconnect.workerconfiguration;

import java.time.Duration;
import java.util.HashMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import software.amazon.awssdk.services.kafkaconnect.KafkaConnectClient;
import software.amazon.awssdk.services.kafkaconnect.model.NotFoundException;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeWorkerConfigurationRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeWorkerConfigurationResponse;
import software.amazon.awssdk.services.kafkaconnect.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.kafkaconnect.model.ListTagsForResourceResponse;
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
public class ReadHandlerTest extends AbstractTestBase {

    @Mock
    private KafkaConnectClient kafkaConnectClient;

    @Mock
    private ExceptionTranslator exceptionTranslator;

    @Mock
    private Translator translator;

    private AmazonWebServicesClientProxy proxy;

    private ProxyClient<KafkaConnectClient> proxyClient;

    private ReadHandler handler;

    @BeforeEach
    public void setup() {
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS,
            () -> Duration.ofSeconds(600).toMillis());
        proxyClient = proxyStub(proxy, kafkaConnectClient);
        handler = new ReadHandler(exceptionTranslator, translator);
    }

    @AfterEach
    public void tear_down() {
        verify(kafkaConnectClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(kafkaConnectClient, exceptionTranslator, translator);
    }

    @Test
    public void handleRequest_returnsCWorkerConfigurationWhenResourceModelIsPassedAndNonEmptyTags_success() {
        when(translator.translateToReadRequest(TestData.RESOURCE_MODEL))
            .thenReturn(TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST,
            kafkaConnectClient::describeWorkerConfiguration))
                .thenReturn(TestData.DESCRIBE_WORKER_CONFIGURATION_RESPONSE);
        when(translator.translateFromReadResponse(TestData.DESCRIBE_WORKER_CONFIGURATION_RESPONSE))
            .thenReturn(TestData.RESPONSE_RESOURCE_MODEL);
        ;
        when(proxyClient.injectCredentialsAndInvokeV2(TestData.LIST_TAGS_FOR_RESOURCE_REQUEST,
            kafkaConnectClient::listTagsForResource)).thenReturn(TestData.LIST_TAGS_FOR_RESOURCE_RESPONSE);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy,
            TestData.RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, logger);

        assertThat(response).isEqualTo(TestData.EXPECTED_RESPONSE);
    }

    @Test

    public void handleRequest_returnsCWorkerConfigurationWhenResourceModelIsPassedAndEmptyTags_success() {
        when(translator.translateToReadRequest(TestData.RESOURCE_MODEL))
            .thenReturn(TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST,
            kafkaConnectClient::describeWorkerConfiguration))
                .thenReturn(TestData.DESCRIBE_WORKER_CONFIGURATION_RESPONSE);
        when(translator.translateFromReadResponse(TestData.DESCRIBE_WORKER_CONFIGURATION_RESPONSE))
            .thenReturn(TestData.RESPONSE_RESOURCE_MODEL);
        ;
        when(proxyClient.injectCredentialsAndInvokeV2(TestData.LIST_TAGS_FOR_RESOURCE_REQUEST,
            kafkaConnectClient::listTagsForResource)).thenReturn(TestData.LIST_TAGS_FOR_RESOURCE_EMPTY_RESPONSE);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy,
            TestData.RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, logger);

        assertThat(response).isEqualTo(TestData.EXPECTED_RESPONSE_EMPTY_TAGS);
    }

    @Test
    public void handleRequest_throwsCfnNotFoundException_whenDescribeWorkerConfigurationFails() {
        final NotFoundException serviceException = NotFoundException.builder().build();
        final CfnNotFoundException cfnException = new CfnNotFoundException(serviceException);
        when(translator.translateToReadRequest(TestData.RESOURCE_MODEL))
            .thenReturn(TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST,
            kafkaConnectClient::describeWorkerConfiguration)).thenThrow(serviceException);
        when(exceptionTranslator.translateToCfnException(serviceException, TestData.WORKER_CONFIGURATION_ARN))
            .thenReturn(cfnException);

        final CfnNotFoundException exception = assertThrows(CfnNotFoundException.class, () -> handler
            .handleRequest(proxy, TestData.RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, logger));

        assertThat(exception).isEqualTo(cfnException);
    }

    @Test
    public void handleRequest_throwsCfnNotFoundException_whenListTagsForResourceFails() {
        final NotFoundException serviceException = NotFoundException.builder().build();
        final CfnNotFoundException cfnException = new CfnNotFoundException(serviceException);

        when(translator.translateToReadRequest(TestData.RESOURCE_MODEL))
            .thenReturn(TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST,
            kafkaConnectClient::describeWorkerConfiguration))
                .thenReturn(TestData.DESCRIBE_WORKER_CONFIGURATION_RESPONSE);
        when(proxyClient.injectCredentialsAndInvokeV2(TestData.LIST_TAGS_FOR_RESOURCE_REQUEST,
            kafkaConnectClient::listTagsForResource)).thenThrow(serviceException);
        when(exceptionTranslator.translateToCfnException(serviceException, TestData.WORKER_CONFIGURATION_ARN))
            .thenReturn(cfnException);

        final CfnNotFoundException exception = assertThrows(CfnNotFoundException.class, () -> handler
            .handleRequest(proxy, TestData.RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, logger));

        assertThat(exception).isEqualTo(cfnException);
    }

    private static class TestData {
        private static final String WORKER_CONFIGURATION_NAME = "unit-test-worker-configuration";
        private static final String WORKER_CONFIGURATION_ARN =
            "arn:aws:kafkaconnect:us-east-1:1111111111:worker-configuration/unit-test-worker-configuration";
        private static final String EXCEPTION_MESSAGE = "Exception message";

        private static final ResourceModel RESPONSE_RESOURCE_MODEL = ResourceModel
            .builder()
            .workerConfigurationArn(WORKER_CONFIGURATION_ARN)
            .name(WORKER_CONFIGURATION_NAME)
            .build();

        private static final ResourceModel RESOURCE_MODEL = ResourceModel
            .builder()
            .workerConfigurationArn(WORKER_CONFIGURATION_ARN)
            .build();

        private static final ResourceHandlerRequest<ResourceModel> RESOURCE_HANDLER_REQUEST =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL)
                .build();

        private static final DescribeWorkerConfigurationRequest DESCRIBE_WORKER_CONFIGURATION_REQUEST =
            DescribeWorkerConfigurationRequest.builder()
                .workerConfigurationArn(WORKER_CONFIGURATION_ARN)
                .build();

        private static final ListTagsForResourceRequest LIST_TAGS_FOR_RESOURCE_REQUEST =
            ListTagsForResourceRequest.builder()
                .resourceArn(WORKER_CONFIGURATION_ARN)
                .build();

        private static final DescribeWorkerConfigurationResponse DESCRIBE_WORKER_CONFIGURATION_RESPONSE =
            DescribeWorkerConfigurationResponse
                .builder()
                .workerConfigurationArn(WORKER_CONFIGURATION_ARN)
                .name(WORKER_CONFIGURATION_NAME)
                .build();

        private static final ListTagsForResourceResponse LIST_TAGS_FOR_RESOURCE_RESPONSE =
            ListTagsForResourceResponse
                .builder()
                .tags(TAGS)
                .build();

        private static final ListTagsForResourceResponse LIST_TAGS_FOR_RESOURCE_EMPTY_RESPONSE =
            ListTagsForResourceResponse
                .builder()
                .tags(new HashMap<>())
                .build();
        private static final ProgressEvent<ResourceModel, CallbackContext> EXPECTED_RESPONSE =
            ProgressEvent.<ResourceModel, CallbackContext>builder()
                .status(OperationStatus.SUCCESS)
                .resourceModel(RESPONSE_RESOURCE_MODEL.toBuilder().tags(TagHelper.convertToSet(TAGS)).build())
                .build();

        private static final ProgressEvent<ResourceModel, CallbackContext> EXPECTED_RESPONSE_EMPTY_TAGS =
            ProgressEvent.<ResourceModel, CallbackContext>builder()
                .status(OperationStatus.SUCCESS)
                .resourceModel(RESPONSE_RESOURCE_MODEL)
                .build();
    }
}