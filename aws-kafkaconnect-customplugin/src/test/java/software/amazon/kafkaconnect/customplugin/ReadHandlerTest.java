package software.amazon.kafkaconnect.customplugin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.HashMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.kafkaconnect.KafkaConnectClient;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeCustomPluginRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeCustomPluginResponse;
import software.amazon.awssdk.services.kafkaconnect.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.kafkaconnect.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.kafkaconnect.model.NotFoundException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

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
        proxy =
            new AmazonWebServicesClientProxy(
                logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        proxyClient = proxyStub(proxy, kafkaConnectClient);
        handler = new ReadHandler(exceptionTranslator, translator);
    }

    @AfterEach
    public void tear_down() {
        verify(kafkaConnectClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(kafkaConnectClient, exceptionTranslator, translator);
    }

    @Test
    public void handleRequest_returnsCustomPluginWhenResourceModelIsPassedAndNonEmptyTags_success() {
        when(translator.translateToReadRequest(TestData.RESOURCE_MODEL))
            .thenReturn(TestData.DESCRIBE_CUSTOM_PLUGIN_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DESCRIBE_CUSTOM_PLUGIN_REQUEST, kafkaConnectClient::describeCustomPlugin))
                .thenReturn(TestData.DESCRIBE_CUSTOM_PLUGIN_RESPONSE);
        when(translator.translateFromReadResponse(TestData.DESCRIBE_CUSTOM_PLUGIN_RESPONSE))
            .thenReturn(TestData.RESPONSE_RESOURCE_MODEL);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.LIST_TAGS_FOR_RESOURCE_REQUEST, kafkaConnectClient::listTagsForResource))
                .thenReturn(TestData.LIST_TAGS_FOR_RESOURCE_RESPONSE);

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(
                proxy, TestData.RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel().getCustomPluginArn())
            .isEqualTo(TestData.CUSTOM_PLUGIN_ARN);
        assertThat(response.getResourceModel().getName()).isEqualTo(TestData.CUSTOM_PLUGIN_NAME);
        assertThat(response.getResourceModel().getTags()).isEqualTo(TagHelper.convertToList(TAGS));
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_returnsCustomPluginWhenResourceModelIsPassedAndEmptyTags_success() {
        when(translator.translateToReadRequest(TestData.RESOURCE_MODEL))
            .thenReturn(TestData.DESCRIBE_CUSTOM_PLUGIN_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DESCRIBE_CUSTOM_PLUGIN_REQUEST, kafkaConnectClient::describeCustomPlugin))
                .thenReturn(TestData.DESCRIBE_CUSTOM_PLUGIN_RESPONSE);
        when(translator.translateFromReadResponse(TestData.DESCRIBE_CUSTOM_PLUGIN_RESPONSE))
            .thenReturn(TestData.RESPONSE_RESOURCE_MODEL_EMPTY_TAGS);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.LIST_TAGS_FOR_RESOURCE_REQUEST, kafkaConnectClient::listTagsForResource))
                .thenReturn(TestData.LIST_TAGS_FOR_RESOURCE_RESPONSE_EMPTY_TAGS);

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(
                proxy, TestData.RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel().getCustomPluginArn())
            .isEqualTo(TestData.CUSTOM_PLUGIN_ARN);
        assertThat(response.getResourceModel().getName()).isEqualTo(TestData.CUSTOM_PLUGIN_NAME);
        assertThat(response.getResourceModel().getTags()).isNullOrEmpty();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_throwsCfnNotFoundException_whenDescribeCustomPluginFails() {
        final NotFoundException serviceException = NotFoundException.builder().build();
        final CfnNotFoundException cfnException = new CfnNotFoundException(serviceException);
        when(translator.translateToReadRequest(TestData.RESOURCE_MODEL))
            .thenReturn(TestData.DESCRIBE_CUSTOM_PLUGIN_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DESCRIBE_CUSTOM_PLUGIN_REQUEST, kafkaConnectClient::describeCustomPlugin))
                .thenThrow(serviceException);
        when(exceptionTranslator.translateToCfnException(serviceException, TestData.CUSTOM_PLUGIN_ARN))
            .thenReturn(cfnException);

        final CfnNotFoundException exception =
            assertThrows(
                CfnNotFoundException.class,
                () -> handler.handleRequest(
                    proxy,
                    TestData.RESOURCE_HANDLER_REQUEST,
                    new CallbackContext(),
                    proxyClient,
                    logger));

        assertThat(exception).isEqualTo(cfnException);
    }

    @Test
    public void handleRequest_throwsCfnNotFoundException_whenListTagsForResourceFails() {
        final NotFoundException serviceException = NotFoundException.builder().build();
        final CfnNotFoundException cfnException = new CfnNotFoundException(serviceException);
        when(translator.translateToReadRequest(TestData.RESOURCE_MODEL))
            .thenReturn(TestData.DESCRIBE_CUSTOM_PLUGIN_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DESCRIBE_CUSTOM_PLUGIN_REQUEST, kafkaConnectClient::describeCustomPlugin))
                .thenReturn(TestData.DESCRIBE_CUSTOM_PLUGIN_RESPONSE);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.LIST_TAGS_FOR_RESOURCE_REQUEST, kafkaConnectClient::listTagsForResource))
                .thenThrow(serviceException);
        when(exceptionTranslator.translateToCfnException(serviceException, TestData.CUSTOM_PLUGIN_ARN))
            .thenReturn(cfnException);

        final CfnNotFoundException exception =
            assertThrows(
                CfnNotFoundException.class,
                () -> handler.handleRequest(
                    proxy,
                    TestData.RESOURCE_HANDLER_REQUEST,
                    new CallbackContext(),
                    proxyClient,
                    logger));

        assertThat(exception).isEqualTo(cfnException);
    }

    private static class TestData {
        private static final String CUSTOM_PLUGIN_ARN =
            "arn:aws:kafkaconnect:us-east-1:123456789:custom-plugin/unit-test-custom-plugin";
        private static final String CUSTOM_PLUGIN_NAME = "unit-test-custom-plugin";

        private static final ResourceModel RESPONSE_RESOURCE_MODEL =
            ResourceModel.builder()
                .customPluginArn(CUSTOM_PLUGIN_ARN)
                .name(CUSTOM_PLUGIN_NAME)
                .tags(TagHelper.convertToList(TAGS))
                .build();

        private static final ResourceModel RESPONSE_RESOURCE_MODEL_EMPTY_TAGS =
            ResourceModel.builder().customPluginArn(CUSTOM_PLUGIN_ARN).name(CUSTOM_PLUGIN_NAME).build();

        private static final ResourceModel RESOURCE_MODEL =
            ResourceModel.builder().customPluginArn(CUSTOM_PLUGIN_ARN).build();

        private static final ResourceHandlerRequest<ResourceModel> RESOURCE_HANDLER_REQUEST =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL)
                .build();

        private static final DescribeCustomPluginRequest DESCRIBE_CUSTOM_PLUGIN_REQUEST =
            DescribeCustomPluginRequest.builder().customPluginArn(CUSTOM_PLUGIN_ARN).build();

        private static final DescribeCustomPluginResponse DESCRIBE_CUSTOM_PLUGIN_RESPONSE =
            DescribeCustomPluginResponse.builder()
                .customPluginArn(CUSTOM_PLUGIN_ARN)
                .name(CUSTOM_PLUGIN_NAME)
                .build();

        private static final ListTagsForResourceRequest LIST_TAGS_FOR_RESOURCE_REQUEST =
            ListTagsForResourceRequest.builder().resourceArn(CUSTOM_PLUGIN_ARN).build();

        private static final ListTagsForResourceResponse LIST_TAGS_FOR_RESOURCE_RESPONSE =
            ListTagsForResourceResponse.builder().tags(TAGS).build();

        private static final ListTagsForResourceResponse LIST_TAGS_FOR_RESOURCE_RESPONSE_EMPTY_TAGS =
            ListTagsForResourceResponse.builder().tags(new HashMap<>()).build();
    }
}
