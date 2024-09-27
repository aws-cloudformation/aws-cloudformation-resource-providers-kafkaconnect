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
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.kafkaconnect.KafkaConnectClient;
import software.amazon.awssdk.services.kafkaconnect.model.CustomPluginLocationDescription;
import software.amazon.awssdk.services.kafkaconnect.model.CustomPluginRevisionSummary;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeCustomPluginRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeCustomPluginResponse;
import software.amazon.awssdk.services.kafkaconnect.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.kafkaconnect.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.kafkaconnect.model.S3LocationDescription;
import software.amazon.awssdk.services.kafkaconnect.model.TagResourceRequest;
import software.amazon.awssdk.services.kafkaconnect.model.TagResourceResponse;
import software.amazon.awssdk.services.kafkaconnect.model.UntagResourceRequest;
import software.amazon.awssdk.services.kafkaconnect.model.UntagResourceResponse;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.exceptions.CfnNotUpdatableException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest.ResourceHandlerRequestBuilder;

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
        proxy = new AmazonWebServicesClientProxy(
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
    public void test_handleRequest_updateTags_success_onAddingAndRemovingTags() {
        final ResourceModel previousModel = TestData.RESOURCE_MODEL_1.toBuilder().build();
        final ResourceModel desiredModel = TestData.RESOURCE_MODEL_2.toBuilder().build();
        final DescribeCustomPluginRequest describeCustomPluginRequest = TestData.DESCRIBE_CUSTOM_PLUGIN_REQUEST_1;
        final DescribeCustomPluginResponse describeCustomPluginResponse = TestData.DESCRIBE_CUSTOM_PLUGIN_RESPONSE_1
            .toBuilder().build();
        when(translator.translateToReadRequest(desiredModel)).thenReturn(describeCustomPluginRequest);
        when(proxyClient.injectCredentialsAndInvokeV2(
            describeCustomPluginRequest, kafkaConnectClient::describeCustomPlugin))
                .thenReturn(describeCustomPluginResponse);
        when(proxyClient.client().untagResource(any(UntagResourceRequest.class)))
            .thenReturn(UntagResourceResponse.builder().build());
        when(proxyClient.client().tagResource(any(TagResourceRequest.class)))
            .thenReturn(TagResourceResponse.builder().build());
        when(translator.translateFromReadResponse(describeCustomPluginResponse))
            .thenReturn(desiredModel);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.LIST_TAGS_FOR_RESOURCE_REQUEST_1, kafkaConnectClient::listTagsForResource))
                .thenReturn(TestData.LIST_TAGS_FOR_RESOURCE_RESPONSE_2);

        final ResourceHandlerRequest<ResourceModel> request =
            TestData.createResourceHandlerRequest(desiredModel, previousModel);
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request,
            new CallbackContext(), proxyClient, logger);

        final ProgressEvent<ResourceModel, CallbackContext> expected = ProgressEvent
            .<ResourceModel, CallbackContext>builder()
            .resourceModel(desiredModel)
            .status(OperationStatus.SUCCESS)
            .build();

        assertThat(response).isEqualTo(expected);

        verify(proxyClient.client(), times(2))
            .describeCustomPlugin(any(DescribeCustomPluginRequest.class));
        verify(proxyClient.client(), times(1))
            .listTagsForResource(any(ListTagsForResourceRequest.class));
        verify(proxyClient.client(), times(1))
            .untagResource(any(UntagResourceRequest.class));
        verify(proxyClient.client(), times(1))
            .tagResource(any(TagResourceRequest.class));
    }

    @Test
    public void test_handleRequest_updateTags_success_onAddingTags() {
        final ResourceModel previousModel = TestData.RESOURCE_MODEL_1.toBuilder().build();
        final ResourceModel desiredModel = TestData.RESOURCE_MODEL_3.toBuilder().build();
        final DescribeCustomPluginRequest describeCustomPluginRequest = TestData.DESCRIBE_CUSTOM_PLUGIN_REQUEST_1;
        final DescribeCustomPluginResponse describeCustomPluginResponse = TestData.DESCRIBE_CUSTOM_PLUGIN_RESPONSE_1
            .toBuilder().build();
        when(translator.translateToReadRequest(desiredModel)).thenReturn(describeCustomPluginRequest);
        when(proxyClient.injectCredentialsAndInvokeV2(
            describeCustomPluginRequest, kafkaConnectClient::describeCustomPlugin))
                .thenReturn(describeCustomPluginResponse);
        when(proxyClient.client().tagResource(any(TagResourceRequest.class)))
            .thenReturn(TagResourceResponse.builder().build());
        when(translator.translateFromReadResponse(describeCustomPluginResponse))
            .thenReturn(desiredModel);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.LIST_TAGS_FOR_RESOURCE_REQUEST_1, kafkaConnectClient::listTagsForResource))
                .thenReturn(TestData.LIST_TAGS_FOR_RESOURCE_RESPONSE_3);

        final ResourceHandlerRequest<ResourceModel> request =
            TestData.createResourceHandlerRequest(desiredModel, previousModel);
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request,
            new CallbackContext(), proxyClient, logger);

        final ProgressEvent<ResourceModel, CallbackContext> expected = ProgressEvent
            .<ResourceModel, CallbackContext>builder()
            .resourceModel(desiredModel)
            .status(OperationStatus.SUCCESS)
            .build();

        assertThat(response).isEqualTo(expected);

        verify(proxyClient.client(), times(2))
            .describeCustomPlugin(any(DescribeCustomPluginRequest.class));
        verify(proxyClient.client(), times(1))
            .listTagsForResource(any(ListTagsForResourceRequest.class));
        verify(proxyClient.client(), times(0))
            .untagResource(any(UntagResourceRequest.class));
        verify(proxyClient.client(), times(1))
            .tagResource(any(TagResourceRequest.class));
    }

    @Test
    public void test_handleRequest_updateTags_success_onRemovingTags() {
        final ResourceModel previousModel = TestData.RESOURCE_MODEL_1.toBuilder().build();
        final ResourceModel desiredModel = TestData.RESOURCE_MODEL_4.toBuilder().build();
        final DescribeCustomPluginRequest describeCustomPluginRequest = TestData.DESCRIBE_CUSTOM_PLUGIN_REQUEST_1;
        final DescribeCustomPluginResponse describeCustomPluginResponse = TestData.DESCRIBE_CUSTOM_PLUGIN_RESPONSE_1
            .toBuilder().build();
        when(translator.translateToReadRequest(desiredModel)).thenReturn(describeCustomPluginRequest);
        when(proxyClient.injectCredentialsAndInvokeV2(
            describeCustomPluginRequest, kafkaConnectClient::describeCustomPlugin))
                .thenReturn(describeCustomPluginResponse);
        when(proxyClient.client().untagResource(any(UntagResourceRequest.class)))
            .thenReturn(UntagResourceResponse.builder().build());
        when(translator.translateFromReadResponse(describeCustomPluginResponse))
            .thenReturn(desiredModel);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.LIST_TAGS_FOR_RESOURCE_REQUEST_1, kafkaConnectClient::listTagsForResource))
                .thenReturn(TestData.LIST_TAGS_FOR_RESOURCE_RESPONSE_4);

        final ResourceHandlerRequest<ResourceModel> request =
            TestData.createResourceHandlerRequest(desiredModel, previousModel);
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request,
            new CallbackContext(), proxyClient, logger);

        final ProgressEvent<ResourceModel, CallbackContext> expected = ProgressEvent
            .<ResourceModel, CallbackContext>builder()
            .resourceModel(desiredModel)
            .status(OperationStatus.SUCCESS)
            .build();

        assertThat(response).isEqualTo(expected);

        verify(proxyClient.client(), times(2))
            .describeCustomPlugin(any(DescribeCustomPluginRequest.class));
        verify(proxyClient.client(), times(1))
            .listTagsForResource(any(ListTagsForResourceRequest.class));
        verify(proxyClient.client(), times(1))
            .untagResource(any(UntagResourceRequest.class));
        verify(proxyClient.client(), times(0))
            .tagResource(any(TagResourceRequest.class));
    }

    @Test
    public void test_handleRequest_updateTags_success_onNoChangeToTags() {
        final ResourceModel previousModel = TestData.RESOURCE_MODEL_1.toBuilder().build();
        final ResourceModel desiredModel = TestData.RESOURCE_MODEL_1.toBuilder().build();
        final DescribeCustomPluginRequest describeCustomPluginRequest = TestData.DESCRIBE_CUSTOM_PLUGIN_REQUEST_1;
        final DescribeCustomPluginResponse describeCustomPluginResponse = TestData.DESCRIBE_CUSTOM_PLUGIN_RESPONSE_1
            .toBuilder().build();
        when(translator.translateToReadRequest(desiredModel)).thenReturn(describeCustomPluginRequest);
        when(proxyClient.injectCredentialsAndInvokeV2(
            describeCustomPluginRequest, kafkaConnectClient::describeCustomPlugin))
                .thenReturn(describeCustomPluginResponse);
        when(translator.translateFromReadResponse(describeCustomPluginResponse))
            .thenReturn(desiredModel);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.LIST_TAGS_FOR_RESOURCE_REQUEST_1, kafkaConnectClient::listTagsForResource))
                .thenReturn(TestData.LIST_TAGS_FOR_RESOURCE_RESPONSE_2);

        final ResourceHandlerRequest<ResourceModel> request =
            TestData.createResourceHandlerRequest(desiredModel, previousModel);
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request,
            new CallbackContext(), proxyClient, logger);

        final ProgressEvent<ResourceModel, CallbackContext> expected = ProgressEvent
            .<ResourceModel, CallbackContext>builder()
            .resourceModel(desiredModel)
            .status(OperationStatus.SUCCESS)
            .build();

        assertThat(response).isEqualTo(expected);

        verify(proxyClient.client(), times(2))
            .describeCustomPlugin(any(DescribeCustomPluginRequest.class));
        verify(proxyClient.client(), times(1))
            .listTagsForResource(any(ListTagsForResourceRequest.class));
        verify(proxyClient.client(), times(0))
            .untagResource(any(UntagResourceRequest.class));
        verify(proxyClient.client(), times(0))
            .tagResource(any(TagResourceRequest.class));
    }

    @Test
    public void test_handleRequest_failure_throwsException_onNameChange() {
        final ResourceModel desiredModel = TestData.RESOURCE_MODEL_1.toBuilder().name(TestData.CUSTOM_PLUGIN_NAME_2)
            .build();
        helper_test_handleRequest_failure_throwsException_onNonUpdatableFieldChange(desiredModel);
    }

    @Test
    public void test_handleRequest_failure_throwsException_onDescriptionChange() {
        final ResourceModel desiredModel = TestData.RESOURCE_MODEL_1
            .toBuilder()
            .description(TestData.CUSTOM_PLUGIN_DESCRIPTION_2)
            .build();
        helper_test_handleRequest_failure_throwsException_onNonUpdatableFieldChange(desiredModel);
    }

    @Test
    public void test_handleRequest_failure_throwsException_onContentTypeChange() {
        final ResourceModel desiredModel = TestData.RESOURCE_MODEL_1
            .toBuilder()
            .contentType(TestData.CUSTOM_PLUGIN_CONTENT_TYPE_2)
            .build();
        helper_test_handleRequest_failure_throwsException_onNonUpdatableFieldChange(desiredModel);
    }

    @Test
    public void test_handleRequest_failure_throwsException_onLocationChange() {
        final ResourceModel desiredModel = TestData.RESOURCE_MODEL_1.toBuilder()
            .location(TestData.CUSTOM_PLUGIN_LOCATION_2).build();
        helper_test_handleRequest_failure_throwsException_onNonUpdatableFieldChange(desiredModel);
    }

    @Test
    public void test_handleRequest_failure_throwsException_onCustomPluginArnChange() {
        final ResourceModel desiredModel = TestData.RESOURCE_MODEL_1.toBuilder()
            .customPluginArn(TestData.CUSTOM_PLUGIN_ARN_2).build();
        helper_test_handleRequest_failure_throwsException_onNonUpdatableFieldChange(desiredModel);
    }

    @Test
    public void test_handleRequest_failure_throwsException_onRevisionChange() {
        final ResourceModel desiredModel = TestData.RESOURCE_MODEL_1.toBuilder()
            .revision(TestData.CUSTOM_PLUGIN_REVISION_2).build();
        helper_test_handleRequest_failure_throwsException_onNonUpdatableFieldChange(desiredModel);
    }

    @Test
    public void test_handleRequest_failure_throwsException_onFileDescriptionChange() {
        final ResourceModel desiredModel = TestData.RESOURCE_MODEL_1
            .toBuilder()
            .fileDescription(TestData.CUSTOM_PLUGIN_FILE_DESCRIPTION_2)
            .build();
        helper_test_handleRequest_failure_throwsException_onNonUpdatableFieldChange(desiredModel);
    }

    @Test
    public void test_handleRequest_failure_throwsCfnException_ifCustomPluginArnIsNull() {
        final ResourceModel desiredModel = TestData.RESOURCE_MODEL_1.toBuilder().build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(desiredModel).build();
        final DescribeCustomPluginRequest describeCustomPluginRequest = TestData.DESCRIBE_CUSTOM_PLUGIN_REQUEST_NULL;
        when(translator.translateToReadRequest(desiredModel)).thenReturn(describeCustomPluginRequest);

        assertThrows(
            CfnNotFoundException.class,
            () -> {
                handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);
            });
    }

    @Test
    public void test_handleRequest_failure_throwsCfnException_ifDescribeCustomPluginApiFails() {
        final ResourceModel desiredModel = TestData.RESOURCE_MODEL_1.toBuilder().build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(desiredModel).build();
        final DescribeCustomPluginRequest describeCustomPluginRequest = TestData.DESCRIBE_CUSTOM_PLUGIN_REQUEST_1;
        when(translator.translateToReadRequest(desiredModel)).thenReturn(describeCustomPluginRequest);
        final AwsServiceException exception = AwsServiceException.builder()
            .message(TestData.EXCEPTION_MESSAGE)
            .build();
        when(proxyClient.injectCredentialsAndInvokeV2(describeCustomPluginRequest,
            kafkaConnectClient::describeCustomPlugin)).thenThrow(exception);
        final CfnGeneralServiceException cfnException = new CfnGeneralServiceException(exception);
        when(exceptionTranslator.translateToCfnException(exception, describeCustomPluginRequest.customPluginArn()))
            .thenReturn(cfnException);
        assertThrows(
            CfnGeneralServiceException.class,
            () -> {
                handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);
            });
    }

    @Test
    public void test_handleRequest_failure_throwsCfnException_ifUntagResourceApiFails() {
        final ResourceModel previousModel = TestData.RESOURCE_MODEL_1.toBuilder().build();
        final ResourceModel desiredModel = TestData.RESOURCE_MODEL_2.toBuilder().build();
        final DescribeCustomPluginRequest describeCustomPluginRequest = TestData.DESCRIBE_CUSTOM_PLUGIN_REQUEST_1;
        final DescribeCustomPluginResponse describeCustomPluginResponse = TestData.DESCRIBE_CUSTOM_PLUGIN_RESPONSE_1
            .toBuilder().build();
        when(translator.translateToReadRequest(desiredModel)).thenReturn(describeCustomPluginRequest);
        when(proxyClient.injectCredentialsAndInvokeV2(
            describeCustomPluginRequest, kafkaConnectClient::describeCustomPlugin))
                .thenReturn(describeCustomPluginResponse);
        final AwsServiceException exception = AwsServiceException.builder()
            .message(TestData.EXCEPTION_MESSAGE)
            .build();
        when(proxyClient.client().untagResource(any(UntagResourceRequest.class)))
            .thenThrow(exception);
        final CfnGeneralServiceException cfnException = new CfnGeneralServiceException(exception);
        when(exceptionTranslator.translateToCfnException(exception, describeCustomPluginRequest.customPluginArn()))
            .thenReturn(cfnException);

        final ResourceHandlerRequest<ResourceModel> request =
            TestData.createResourceHandlerRequest(desiredModel, previousModel);
        assertThrows(
            CfnGeneralServiceException.class,
            () -> {
                handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);
            });
    }

    @Test
    public void test_handleRequest_failure_throwsCfnException_ifTagResourceApiFails() {
        final ResourceModel previousModel = TestData.RESOURCE_MODEL_1.toBuilder().build();
        final ResourceModel desiredModel = TestData.RESOURCE_MODEL_2.toBuilder().build();
        final DescribeCustomPluginRequest describeCustomPluginRequest = TestData.DESCRIBE_CUSTOM_PLUGIN_REQUEST_1;
        final DescribeCustomPluginResponse describeCustomPluginResponse = TestData.DESCRIBE_CUSTOM_PLUGIN_RESPONSE_1
            .toBuilder().build();
        when(translator.translateToReadRequest(desiredModel)).thenReturn(describeCustomPluginRequest);
        when(proxyClient.injectCredentialsAndInvokeV2(
            describeCustomPluginRequest, kafkaConnectClient::describeCustomPlugin))
                .thenReturn(describeCustomPluginResponse);
        final AwsServiceException exception = AwsServiceException.builder()
            .message(TestData.EXCEPTION_MESSAGE)
            .build();
        when(proxyClient.client().untagResource(any(UntagResourceRequest.class)))
            .thenReturn(UntagResourceResponse.builder().build());
        when(proxyClient.client().tagResource(any(TagResourceRequest.class)))
            .thenThrow(exception);
        final CfnGeneralServiceException cfnException = new CfnGeneralServiceException(exception);
        when(exceptionTranslator.translateToCfnException(exception, describeCustomPluginRequest.customPluginArn()))
            .thenReturn(cfnException);

        final ResourceHandlerRequest<ResourceModel> request =
            TestData.createResourceHandlerRequest(desiredModel, previousModel);
        assertThrows(
            CfnGeneralServiceException.class,
            () -> {
                handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);
            });
    }

    // This is a helper method, please don't add @Test annotation.
    private void helper_test_handleRequest_failure_throwsException_onNonUpdatableFieldChange(
        ResourceModel desiredModel) {
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(desiredModel)
            .desiredResourceTags(TagHelper.convertToMap(desiredModel.getTags()))
            .previousResourceState(TestData.RESOURCE_MODEL_1.toBuilder().build())
            .build();
        final DescribeCustomPluginRequest describeCustomPluginRequest = TestData.DESCRIBE_CUSTOM_PLUGIN_REQUEST_1;
        final DescribeCustomPluginResponse describeCustomPluginResponse = TestData.DESCRIBE_CUSTOM_PLUGIN_RESPONSE_1
            .toBuilder().build();
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
        private static final String CUSTOM_PLUGIN_DESCRIPTION_1 = "unit-test-custom-plugin-description-1";
        private static final String CUSTOM_PLUGIN_CONTENT_TYPE_1 = "unit-test-custom-plugin-content-type-1";
        private static final String CUSTOM_PLUGIN_BUCKET_ARN_1 = "unit-test-custom-plugin-bucket-arn-1";
        private static final String CUSTOM_PLUGIN_FILE_KEY_1 = "unit-test-custom-plugin-file-key-1";
        private static final String CUSTOM_PLUGIN_FILE_MD5_1 = "unit-test-custom-plugin-file-md5-1";
        private static final Long CUSTOM_PLUGIN_FILE_SIZE_1 = 123L;
        private static final Long CUSTOM_PLUGIN_REVISION_1 = 1L;

        private static final String CUSTOM_PLUGIN_ARN_2 =
            "arn:aws:kafkaconnect:us-east-1:123456789:custom-plugin/unit-test-custom-plugin-2";
        private static final String CUSTOM_PLUGIN_NAME_2 = "unit-test-custom-plugin-2";
        private static final String CUSTOM_PLUGIN_DESCRIPTION_2 = "unit-test-custom-plugin-description-2";
        private static final String CUSTOM_PLUGIN_CONTENT_TYPE_2 = "unit-test-custom-plugin-content-type-2";
        private static final String CUSTOM_PLUGIN_BUCKET_ARN_2 = "unit-test-custom-plugin-bucket-arn-2";
        private static final String CUSTOM_PLUGIN_FILE_KEY_2 = "unit-test-custom-plugin-file-key-2";
        private static final String CUSTOM_PLUGIN_FILE_MD5_2 = "unit-test-custom-plugin-file-md5-2";
        private static final Long CUSTOM_PLUGIN_FILE_SIZE_2 = 456L;
        private static final Long CUSTOM_PLUGIN_REVISION_2 = 2L;
        private static final String EXCEPTION_MESSAGE = "exception-message";

        private static final CustomPluginLocation CUSTOM_PLUGIN_LOCATION_1 = CustomPluginLocation.builder()
            .s3Location(
                S3Location.builder()
                    .bucketArn(CUSTOM_PLUGIN_BUCKET_ARN_1)
                    .fileKey(CUSTOM_PLUGIN_FILE_KEY_1)
                    .build())
            .build();
        private static final CustomPluginLocation CUSTOM_PLUGIN_LOCATION_2 = CustomPluginLocation.builder()
            .s3Location(
                S3Location.builder()
                    .bucketArn(CUSTOM_PLUGIN_BUCKET_ARN_2)
                    .fileKey(CUSTOM_PLUGIN_FILE_KEY_2)
                    .build())
            .build();

        private static final CustomPluginFileDescription CUSTOM_PLUGIN_FILE_DESCRIPTION_1 = CustomPluginFileDescription
            .builder()
            .fileMd5(CUSTOM_PLUGIN_FILE_MD5_1)
            .fileSize(CUSTOM_PLUGIN_FILE_SIZE_1)
            .build();
        private static final CustomPluginFileDescription CUSTOM_PLUGIN_FILE_DESCRIPTION_2 = CustomPluginFileDescription
            .builder()
            .fileMd5(CUSTOM_PLUGIN_FILE_MD5_2)
            .fileSize(CUSTOM_PLUGIN_FILE_SIZE_2)
            .build();

        private static final Map<String, String> SYSTEM_TAGS = new HashMap<String, String>() {
            {
                put("SYSTEM_TAG_TEST1", "SYSTEM_TAG_TEST_VALUE1");
                put("SYSTEM_TAG_TEST2", "SYSTEM_TAG_TEST_VALUE2");
            }
        };

        private static final Map<String, String> TAGS_1 = new HashMap<String, String>() {
            {
                put("TEST_TAG1", "TEST_TAG_VALUE1");
                put("TEST_TAG2", "TEST_TAG_VALUE2");
            }
        };

        private static final Map<String, String> TAGS_2 = new HashMap<String, String>() {
            {
                put("TEST_TAG1", "TEST_TAG_VALUE1");
                put("TEST_TAG3", "TEST_TAG_VALUE3");
            }
        };

        private static final Map<String, String> TAGS_3 = new HashMap<String, String>() {
            {
                put("TEST_TAG1", "TEST_TAG_VALUE1");
                put("TEST_TAG2", "TEST_TAG_VALUE2");
                put("TEST_TAG3", "TEST_TAG_VALUE3");
            }
        };

        private static final Map<String, String> TAGS_4 = new HashMap<String, String>() {
            {
                put("TEST_TAG1", "TEST_TAG_VALUE1");
            }
        };

        private static final ResourceModel RESOURCE_MODEL_1 = ResourceModel.builder()
            .customPluginArn(CUSTOM_PLUGIN_ARN_1)
            .name(CUSTOM_PLUGIN_NAME_1)
            .description(CUSTOM_PLUGIN_DESCRIPTION_1)
            .contentType(CUSTOM_PLUGIN_CONTENT_TYPE_1)
            .location(CUSTOM_PLUGIN_LOCATION_1)
            .revision(CUSTOM_PLUGIN_REVISION_1)
            .fileDescription(CUSTOM_PLUGIN_FILE_DESCRIPTION_1)
            .tags(TagHelper.convertToList(TAGS_1))
            .build();

        private static final ResourceModel RESOURCE_MODEL_2 = RESOURCE_MODEL_1.toBuilder()
            .tags(TagHelper.convertToList(TAGS_2)).build();

        private static final ResourceModel RESOURCE_MODEL_3 = RESOURCE_MODEL_1.toBuilder()
            .tags(TagHelper.convertToList(TAGS_3)).build();

        private static final ResourceModel RESOURCE_MODEL_4 = RESOURCE_MODEL_1.toBuilder()
            .tags(TagHelper.convertToList(TAGS_4)).build();

        private static final DescribeCustomPluginRequest DESCRIBE_CUSTOM_PLUGIN_REQUEST_1 = DescribeCustomPluginRequest
            .builder().customPluginArn(CUSTOM_PLUGIN_ARN_1).build();
        private static final DescribeCustomPluginResponse DESCRIBE_CUSTOM_PLUGIN_RESPONSE_1 =
            DescribeCustomPluginResponse
                .builder()
                .customPluginArn(CUSTOM_PLUGIN_ARN_1)
                .name(CUSTOM_PLUGIN_NAME_1)
                .description(CUSTOM_PLUGIN_DESCRIPTION_1)
                .latestRevision(
                    CustomPluginRevisionSummary.builder()
                        .revision(CUSTOM_PLUGIN_REVISION_1)
                        .contentType(CUSTOM_PLUGIN_CONTENT_TYPE_1)
                        .fileDescription(
                            software.amazon.awssdk.services.kafkaconnect.model.CustomPluginFileDescription
                                .builder()
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

        private static final DescribeCustomPluginRequest DESCRIBE_CUSTOM_PLUGIN_REQUEST_NULL =
            DescribeCustomPluginRequest
                .builder().customPluginArn(null).build();

        private static final ListTagsForResourceRequest LIST_TAGS_FOR_RESOURCE_REQUEST_1 = ListTagsForResourceRequest
            .builder().resourceArn(CUSTOM_PLUGIN_ARN_1).build();

        private static final ListTagsForResourceResponse LIST_TAGS_FOR_RESOURCE_RESPONSE_2 = ListTagsForResourceResponse
            .builder().tags(TAGS_2).build();
        private static final ListTagsForResourceResponse LIST_TAGS_FOR_RESOURCE_RESPONSE_3 = ListTagsForResourceResponse
            .builder().tags(TAGS_3).build();
        private static final ListTagsForResourceResponse LIST_TAGS_FOR_RESOURCE_RESPONSE_4 = ListTagsForResourceResponse
            .builder().tags(TAGS_4).build();

        private static ResourceHandlerRequest<ResourceModel> createResourceHandlerRequest(
            @Nonnull ResourceModel desiredModel,
            @Nullable ResourceModel previousModel) {
            final ResourceHandlerRequestBuilder<ResourceModel> requestBuilder =
                ResourceHandlerRequest.<ResourceModel>builder()
                    .desiredResourceState(desiredModel)
                    .desiredResourceTags(TagHelper.convertToMap(desiredModel.getTags()))
                    .systemTags(TestData.SYSTEM_TAGS);

            if (previousModel != null) {
                requestBuilder.previousResourceState(previousModel)
                    .previousResourceTags(TagHelper.convertToMap(previousModel.getTags()))
                    .previousSystemTags(TestData.SYSTEM_TAGS);
            }

            return requestBuilder.build();
        }
    }
}
