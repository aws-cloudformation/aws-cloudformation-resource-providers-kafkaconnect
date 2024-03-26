package software.amazon.kafkaconnect.customplugin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.kafkaconnect.KafkaConnectClient;
import software.amazon.awssdk.services.kafkaconnect.model.CustomPluginLocationDescription;
import software.amazon.awssdk.services.kafkaconnect.model.CustomPluginRevisionSummary;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeCustomPluginRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeCustomPluginResponse;
import software.amazon.awssdk.services.kafkaconnect.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.kafkaconnect.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.kafkaconnect.model.S3LocationDescription;
import software.amazon.cloudformation.exceptions.CfnNotUpdatableException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

@ExtendWith(MockitoExtension.class)
public class UpdateHandlerTest extends AbstractTestBase {
    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<KafkaConnectClient> proxyClient;

    @Mock
    KafkaConnectClient kafkaConnectClient;

    @Mock
    private ExceptionTranslator exceptionTranslator;

    @Mock
    private Translator translator;

    private ReadHandler readHandler;

    private UpdateHandler handler;

    @BeforeEach
    public void setup() {
        proxy =
            new AmazonWebServicesClientProxy(
                logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        proxyClient = proxyStub(proxy, kafkaConnectClient);
        readHandler = new ReadHandler(exceptionTranslator, translator);
        handler = new UpdateHandler(exceptionTranslator, translator, readHandler);
    }

    @AfterEach
    public void tear_down() {
        verify(kafkaConnectClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(kafkaConnectClient, exceptionTranslator, translator);
    }

    @Test
    public void test_handleRequest_success() {
        final ResourceModel desiredModel = TestData.RESOURCE_MODEL_1.toBuilder().build();
        final ResourceHandlerRequest<ResourceModel> request =
            ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(desiredModel).build();
        final DescribeCustomPluginRequest describeCustomPluginRequest =
            TestData.DESCRIBE_CUSTOM_PLUGIN_REQUEST_1;
        final DescribeCustomPluginResponse describeCustomPluginResponse =
            TestData.DESCRIBE_CUSTOM_PLUGIN_RESPONSE_1.toBuilder().build();
        when(translator.translateToReadRequest(desiredModel)).thenReturn(describeCustomPluginRequest);
        when(proxyClient.injectCredentialsAndInvokeV2(
            describeCustomPluginRequest, kafkaConnectClient::describeCustomPlugin))
                .thenReturn(describeCustomPluginResponse);
        when(translator.translateFromReadResponse(describeCustomPluginResponse))
            .thenReturn(desiredModel);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.LIST_TAGS_FOR_RESOURCE_REQUEST_1, kafkaConnectClient::listTagsForResource))
                .thenReturn(TestData.LIST_TAGS_FOR_RESOURCE_RESPONSE);

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel().getCustomPluginArn())
            .isEqualTo(TestData.CUSTOM_PLUGIN_ARN_1);
        assertThat(response.getResourceModel().getName()).isEqualTo(TestData.CUSTOM_PLUGIN_NAME_1);
        assertThat(response.getResourceModel().getTags()).isEqualTo(TagHelper.convertToList(TAGS));
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(2))
            .describeCustomPlugin(any(DescribeCustomPluginRequest.class));
        verify(proxyClient.client(), times(1))
            .listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void test_handleRequest_failure_throwsException_onNameChange() {
        final ResourceModel desiredModel =
            TestData.RESOURCE_MODEL_1.toBuilder().name(TestData.CUSTOM_PLUGIN_NAME_2).build();
        helper_test_handleRequest_failure_throwsException_onNonUpdatableFieldChange(desiredModel);
    }

    @Test
    public void test_handleRequest_failure_throwsException_onDescriptionChange() {
        final ResourceModel desiredModel =
            TestData.RESOURCE_MODEL_1
                .toBuilder()
                .description(TestData.CUSTOM_PLUGIN_DESCRIPTION_2)
                .build();
        helper_test_handleRequest_failure_throwsException_onNonUpdatableFieldChange(desiredModel);
    }

    @Test
    public void test_handleRequest_failure_throwsException_onContentTypeChange() {
        final ResourceModel desiredModel =
            TestData.RESOURCE_MODEL_1
                .toBuilder()
                .contentType(TestData.CUSTOM_PLUGIN_CONTENT_TYPE_2)
                .build();
        helper_test_handleRequest_failure_throwsException_onNonUpdatableFieldChange(desiredModel);
    }

    @Test
    public void test_handleRequest_failure_throwsException_onLocationChange() {
        final ResourceModel desiredModel =
            TestData.RESOURCE_MODEL_1.toBuilder().location(TestData.CUSTOM_PLUGIN_LOCATION_2).build();
        helper_test_handleRequest_failure_throwsException_onNonUpdatableFieldChange(desiredModel);
    }

    @Test
    public void test_handleRequest_failure_throwsException_onCustomPluginArnChange() {
        final ResourceModel desiredModel =
            TestData.RESOURCE_MODEL_1.toBuilder().customPluginArn(TestData.CUSTOM_PLUGIN_ARN_2).build();
        helper_test_handleRequest_failure_throwsException_onNonUpdatableFieldChange(desiredModel);
    }

    @Test
    public void test_handleRequest_failure_throwsException_onRevisionChange() {
        final ResourceModel desiredModel =
            TestData.RESOURCE_MODEL_1.toBuilder().revision(TestData.CUSTOM_PLUGIN_REVISION_2).build();
        helper_test_handleRequest_failure_throwsException_onNonUpdatableFieldChange(desiredModel);
    }

    @Test
    public void test_handleRequest_failure_throwsException_onFileDescriptionChange() {
        final ResourceModel desiredModel =
            TestData.RESOURCE_MODEL_1
                .toBuilder()
                .fileDescription(TestData.CUSTOM_PLUGIN_FILE_DESCRIPTION_2)
                .build();
        helper_test_handleRequest_failure_throwsException_onNonUpdatableFieldChange(desiredModel);
    }

    // This is a helper method, please don't add @Test annotation.
    private void helper_test_handleRequest_failure_throwsException_onNonUpdatableFieldChange(
        ResourceModel desiredModel) {
        final ResourceHandlerRequest<ResourceModel> request =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(desiredModel)
                .previousResourceState(TestData.RESOURCE_MODEL_1.toBuilder().build())
                .build();
        final DescribeCustomPluginRequest describeCustomPluginRequest =
            TestData.DESCRIBE_CUSTOM_PLUGIN_REQUEST_1;
        final DescribeCustomPluginResponse describeCustomPluginResponse =
            TestData.DESCRIBE_CUSTOM_PLUGIN_RESPONSE_1.toBuilder().build();
        when(translator.translateToReadRequest(desiredModel)).thenReturn(describeCustomPluginRequest);
        when(proxyClient.injectCredentialsAndInvokeV2(
            describeCustomPluginRequest, kafkaConnectClient::describeCustomPlugin))
                .thenReturn(describeCustomPluginResponse);

        assertThrows(
            CfnNotUpdatableException.class,
            () -> {
                handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);
            });
    }

    private static class TestData {
        private static final String CUSTOM_PLUGIN_ARN_1 =
            "arn:aws:kafkaconnect:us-east-1:123456789:custom-plugin/unit-test-custom-plugin-1";
        private static final String CUSTOM_PLUGIN_NAME_1 = "unit-test-custom-plugin-1";
        private static final String CUSTOM_PLUGIN_DESCRIPTION_1 =
            "unit-test-custom-plugin-description-1";
        private static final String CUSTOM_PLUGIN_CONTENT_TYPE_1 =
            "unit-test-custom-plugin-content-type-1";
        private static final String CUSTOM_PLUGIN_BUCKET_ARN_1 = "unit-test-custom-plugin-bucket-arn-1";
        private static final String CUSTOM_PLUGIN_FILE_KEY_1 = "unit-test-custom-plugin-file-key-1";
        private static final String CUSTOM_PLUGIN_FILE_MD5_1 = "unit-test-custom-plugin-file-md5-1";
        private static final Long CUSTOM_PLUGIN_FILE_SIZE_1 = 123L;
        private static final Long CUSTOM_PLUGIN_REVISION_1 = 1L;

        private static final String CUSTOM_PLUGIN_ARN_2 =
            "arn:aws:kafkaconnect:us-east-1:123456789:custom-plugin/unit-test-custom-plugin-2";
        private static final String CUSTOM_PLUGIN_NAME_2 = "unit-test-custom-plugin-2";
        private static final String CUSTOM_PLUGIN_DESCRIPTION_2 =
            "unit-test-custom-plugin-description-2";
        private static final String CUSTOM_PLUGIN_CONTENT_TYPE_2 =
            "unit-test-custom-plugin-content-type-2";
        private static final String CUSTOM_PLUGIN_BUCKET_ARN_2 = "unit-test-custom-plugin-bucket-arn-2";
        private static final String CUSTOM_PLUGIN_FILE_KEY_2 = "unit-test-custom-plugin-file-key-2";
        private static final String CUSTOM_PLUGIN_FILE_MD5_2 = "unit-test-custom-plugin-file-md5-2";
        private static final Long CUSTOM_PLUGIN_FILE_SIZE_2 = 456L;
        private static final Long CUSTOM_PLUGIN_REVISION_2 = 2L;

        private static final CustomPluginLocation CUSTOM_PLUGIN_LOCATION_1 =
            CustomPluginLocation.builder()
                .s3Location(
                    S3Location.builder()
                        .bucketArn(CUSTOM_PLUGIN_BUCKET_ARN_1)
                        .fileKey(CUSTOM_PLUGIN_FILE_KEY_1)
                        .build())
                .build();
        private static final CustomPluginLocation CUSTOM_PLUGIN_LOCATION_2 =
            CustomPluginLocation.builder()
                .s3Location(
                    S3Location.builder()
                        .bucketArn(CUSTOM_PLUGIN_BUCKET_ARN_2)
                        .fileKey(CUSTOM_PLUGIN_FILE_KEY_2)
                        .build())
                .build();

        private static final CustomPluginFileDescription CUSTOM_PLUGIN_FILE_DESCRIPTION_1 =
            CustomPluginFileDescription.builder()
                .fileMd5(CUSTOM_PLUGIN_FILE_MD5_1)
                .fileSize(CUSTOM_PLUGIN_FILE_SIZE_1)
                .build();
        private static final CustomPluginFileDescription CUSTOM_PLUGIN_FILE_DESCRIPTION_2 =
            CustomPluginFileDescription.builder()
                .fileMd5(CUSTOM_PLUGIN_FILE_MD5_2)
                .fileSize(CUSTOM_PLUGIN_FILE_SIZE_2)
                .build();

        private static final ResourceModel RESOURCE_MODEL_1 =
            ResourceModel.builder()
                .customPluginArn(CUSTOM_PLUGIN_ARN_1)
                .name(CUSTOM_PLUGIN_NAME_1)
                .description(CUSTOM_PLUGIN_DESCRIPTION_1)
                .contentType(CUSTOM_PLUGIN_CONTENT_TYPE_1)
                .location(CUSTOM_PLUGIN_LOCATION_1)
                .revision(CUSTOM_PLUGIN_REVISION_1)
                .fileDescription(CUSTOM_PLUGIN_FILE_DESCRIPTION_1)
                .build();

        private static final DescribeCustomPluginRequest DESCRIBE_CUSTOM_PLUGIN_REQUEST_1 =
            DescribeCustomPluginRequest.builder().customPluginArn(CUSTOM_PLUGIN_ARN_1).build();
        private static final DescribeCustomPluginResponse DESCRIBE_CUSTOM_PLUGIN_RESPONSE_1 =
            DescribeCustomPluginResponse.builder()
                .customPluginArn(CUSTOM_PLUGIN_ARN_1)
                .name(CUSTOM_PLUGIN_NAME_1)
                .description(CUSTOM_PLUGIN_DESCRIPTION_1)
                .latestRevision(
                    CustomPluginRevisionSummary.builder()
                        .revision(CUSTOM_PLUGIN_REVISION_1)
                        .contentType(CUSTOM_PLUGIN_CONTENT_TYPE_1)
                        .fileDescription(
                            software.amazon.awssdk.services.kafkaconnect.model.CustomPluginFileDescription.builder()
                                .fileMd5(CUSTOM_PLUGIN_FILE_MD5_1)
                                .fileSize(CUSTOM_PLUGIN_FILE_SIZE_1)
                                .build())
                        .location(
                            CustomPluginLocationDescription.builder()
                                .s3Location(
                                    S3LocationDescription.builder()
                                        .bucketArn(CUSTOM_PLUGIN_BUCKET_ARN_1)
                                        .fileKey(CUSTOM_PLUGIN_FILE_KEY_1)
                                        .build())
                                .build())
                        .build())
                .build();

        private static final ListTagsForResourceRequest LIST_TAGS_FOR_RESOURCE_REQUEST_1 =
            ListTagsForResourceRequest.builder().resourceArn(CUSTOM_PLUGIN_ARN_1).build();

        private static final ListTagsForResourceResponse LIST_TAGS_FOR_RESOURCE_RESPONSE =
            ListTagsForResourceResponse.builder().tags(TAGS).build();
    }
}
