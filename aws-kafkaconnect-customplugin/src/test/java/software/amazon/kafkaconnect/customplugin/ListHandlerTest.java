package software.amazon.kafkaconnect.customplugin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.kafkaconnect.KafkaConnectClient;
import software.amazon.awssdk.services.kafkaconnect.model.CustomPluginSummary;
import software.amazon.awssdk.services.kafkaconnect.model.ListCustomPluginsRequest;
import software.amazon.awssdk.services.kafkaconnect.model.ListCustomPluginsResponse;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

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
        proxy =
            new AmazonWebServicesClientProxy(
                logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        proxyClient = proxyStub(proxy, kafkaConnectClient);
        handler = new ListHandler(exceptionTranslator, translator);
    }

    @AfterEach
    public void tear_down() {
        verifyNoMoreInteractions(kafkaConnectClient, exceptionTranslator, translator);
    }

    @Test
    public void handleRequest_success() {
        when(translator.translateToListRequest(TestData.NEXT_TOKEN_1))
            .thenReturn(TestData.LIST_CUSTOM_PLUGINS_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.LIST_CUSTOM_PLUGINS_REQUEST, kafkaConnectClient::listCustomPlugins))
                .thenReturn(TestData.LIST_CUSTOM_PLUGINS_RESPONSE);
        when(translator.translateFromListResponse(TestData.LIST_CUSTOM_PLUGINS_RESPONSE))
            .thenReturn(TestData.CUSTOM_PLUGIN_MODELS);

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(
                proxy, TestData.RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, logger);

        assertThat(response).isEqualTo(TestData.SUCCESS_RESPONSE);
    }

    @Test
    public void handleRequest_throwsException_whenListCustomPluginsFails() {
        final AwsServiceException serviceException = AwsServiceException.builder().build();
        final CfnGeneralServiceException cfnException =
            new CfnGeneralServiceException(serviceException);
        when(translator.translateToListRequest(TestData.NEXT_TOKEN_1))
            .thenReturn(TestData.LIST_CUSTOM_PLUGINS_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.LIST_CUSTOM_PLUGINS_REQUEST, kafkaConnectClient::listCustomPlugins))
                .thenThrow(serviceException);
        when(exceptionTranslator.translateToCfnException(serviceException, TestData.AWS_ACCOUNT_ID))
            .thenReturn(cfnException);

        final CfnGeneralServiceException exception =
            assertThrows(
                CfnGeneralServiceException.class,
                () -> handler.handleRequest(
                    proxy,
                    TestData.RESOURCE_HANDLER_REQUEST,
                    new CallbackContext(),
                    proxyClient,
                    logger));

        assertThat(exception).isEqualTo(cfnException);
    }

    private static class TestData {
        private static final String NEXT_TOKEN_1 = "next-token-1";
        private static final String NEXT_TOKEN_2 = "next-token-2";
        private static final String AWS_ACCOUNT_ID = "1111111111";
        private static final String CUSTOM_PLUGIN_ARN_1 =
            "arn:aws:kafkaconnect:us-east-1:123456789:custom-plugin/unit-test-custom-plugin-1";
        private static final String CUSTOM_PLUGIN_ARN_2 =
            "arn:aws:kafkaconnect:us-east-1:123456789:custom-plugin/unit-test-custom-plugin-2";
        private static final String CUSTOM_PLUGIN_NAME_1 = "unit-test-custom-plugin-1";
        private static final String CUSTOM_PLUGIN_NAME_2 = "unit-test-custom-plugin-2";

        private static final ListCustomPluginsRequest LIST_CUSTOM_PLUGINS_REQUEST =
            ListCustomPluginsRequest.builder().nextToken(NEXT_TOKEN_1).build();

        private static final ResourceModel MODEL = ResourceModel.builder().build();

        private static final ResourceHandlerRequest<ResourceModel> RESOURCE_HANDLER_REQUEST =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(MODEL)
                .nextToken(NEXT_TOKEN_1)
                .awsAccountId(AWS_ACCOUNT_ID)
                .build();

        public static final List<ResourceModel> CUSTOM_PLUGIN_MODELS =
            Arrays.asList(
                buildResourceModel(CUSTOM_PLUGIN_NAME_1, CUSTOM_PLUGIN_ARN_1),
                buildResourceModel(CUSTOM_PLUGIN_NAME_2, CUSTOM_PLUGIN_ARN_2));

        private static final List<CustomPluginSummary> CUSTOM_PLUGIN_SUMMARIES =
            Arrays.asList(
                buildCustomPluginSummary(CUSTOM_PLUGIN_NAME_1, CUSTOM_PLUGIN_ARN_1),
                buildCustomPluginSummary(CUSTOM_PLUGIN_NAME_2, CUSTOM_PLUGIN_ARN_2));

        private static final ProgressEvent<ResourceModel, CallbackContext> SUCCESS_RESPONSE =
            ProgressEvent.<ResourceModel, CallbackContext>builder()
                .resourceModels(CUSTOM_PLUGIN_MODELS)
                .nextToken(NEXT_TOKEN_2)
                .status(OperationStatus.SUCCESS)
                .build();

        private static final ListCustomPluginsResponse LIST_CUSTOM_PLUGINS_RESPONSE =
            ListCustomPluginsResponse.builder()
                .nextToken(TestData.NEXT_TOKEN_2)
                .customPlugins(TestData.CUSTOM_PLUGIN_SUMMARIES)
                .build();

        private static final CustomPluginSummary buildCustomPluginSummary(
            final String customPluginName, final String customPluginArn) {
            return CustomPluginSummary.builder()
                .customPluginArn(customPluginArn)
                .name(customPluginName)
                .build();
        }

        private static ResourceModel buildResourceModel(
            final String customPluginName, final String customPluginArn) {
            return ResourceModel.builder()
                .customPluginArn(customPluginArn)
                .name(customPluginName)
                .build();
        }
    }
}
