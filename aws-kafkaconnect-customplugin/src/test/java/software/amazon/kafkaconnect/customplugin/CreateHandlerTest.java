package software.amazon.kafkaconnect.customplugin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.kafkaconnect.KafkaConnectClient;
import software.amazon.awssdk.services.kafkaconnect.model.ConflictException;
import software.amazon.awssdk.services.kafkaconnect.model.CreateCustomPluginRequest;
import software.amazon.awssdk.services.kafkaconnect.model.CreateCustomPluginResponse;
import software.amazon.awssdk.services.kafkaconnect.model.CustomPluginContentType;
import software.amazon.awssdk.services.kafkaconnect.model.CustomPluginLocation;
import software.amazon.awssdk.services.kafkaconnect.model.CustomPluginLocationDescription;
import software.amazon.awssdk.services.kafkaconnect.model.CustomPluginRevisionSummary;
import software.amazon.awssdk.services.kafkaconnect.model.CustomPluginState;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeCustomPluginRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeCustomPluginResponse;
import software.amazon.awssdk.services.kafkaconnect.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.kafkaconnect.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.kafkaconnect.model.S3Location;
import software.amazon.awssdk.services.kafkaconnect.model.S3LocationDescription;
import software.amazon.awssdk.services.kafkaconnect.model.StateDescription;
import software.amazon.cloudformation.exceptions.CfnAlreadyExistsException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnResourceConflictException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

@ExtendWith(MockitoExtension.class)
public class CreateHandlerTest extends AbstractTestBase {

    @Mock
    private KafkaConnectClient kafkaConnectClient;

    @Mock
    private ExceptionTranslator exceptionTranslator;

    @Mock
    private Translator translator;

    private ReadHandler readHandler;

    private AmazonWebServicesClientProxy proxy;

    private ProxyClient<KafkaConnectClient> proxyClient;

    private CreateHandler handler;

    @BeforeEach
    public void setup() {
        proxy =
            new AmazonWebServicesClientProxy(
                logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        proxyClient = proxyStub(proxy, kafkaConnectClient);
        readHandler = new ReadHandler(exceptionTranslator, translator);
        handler = new CreateHandler(exceptionTranslator, translator, readHandler);
    }

    @AfterEach
    public void tear_down() {
        verify(kafkaConnectClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(kafkaConnectClient, exceptionTranslator, translator);
    }

    @Test
    public void handleRequest_success() {
        final ResourceModel resourceModel = TestData.getResourceModel();
        when(translator.translateToCreateRequest(resourceModel, TagHelper.convertToMap(resourceModel.getTags())))
            .thenReturn(TestData.CREATE_CUSTOM_PLUGIN_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.CREATE_CUSTOM_PLUGIN_REQUEST, kafkaConnectClient::createCustomPlugin))
                .thenReturn(TestData.CREATE_CUSTOM_PLUGIN_RESPONSE);
        when(translator.translateToReadRequest(TestData.RESOURCE_MODEL_WITH_ARN))
            .thenReturn(TestData.DESCRIBE_CUSTOM_PLUGIN_REQUEST);
        final DescribeCustomPluginResponse describeCustomPluginResponse =
            TestData.FULL_DESCRIBE_CUSTOM_PLUGIN_RESPONSE;
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DESCRIBE_CUSTOM_PLUGIN_REQUEST, kafkaConnectClient::describeCustomPlugin))
                .thenReturn(
                    describeCustomPluginResponse.toBuilder().customPluginState(CustomPluginState.CREATING).build())
                .thenReturn(describeCustomPluginResponse);
        when(translator.translateFromReadResponse(describeCustomPluginResponse))
            .thenReturn(TestData.RESOURCE_MODEL_WITH_ARN);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.LIST_TAGS_FOR_RESOURCE_REQUEST, kafkaConnectClient::listTagsForResource))
                .thenReturn(TestData.LIST_TAGS_FOR_RESOURCE_RESPONSE);

        final ResourceHandlerRequest<ResourceModel> request =
            TestData.getResourceHandlerRequest(resourceModel);
        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isEqualTo(TestData.DESCRIBE_RESPONSE);
        assertThat(response.getResourceModel().getTags())
            .isEqualTo(request.getDesiredResourceState().getTags());
    }

    @Test
    public void handleRequest_throwsAlreadyExistsException_whenCustomPluginExists() {
        final ResourceModel resourceModel = TestData.getResourceModel();
        final ConflictException cException = ConflictException.builder().build();
        final CfnAlreadyExistsException cfnException = new CfnAlreadyExistsException(cException);
        when(translator.translateToCreateRequest(resourceModel, TagHelper.convertToMap(resourceModel.getTags())))
            .thenReturn(TestData.CREATE_CUSTOM_PLUGIN_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.CREATE_CUSTOM_PLUGIN_REQUEST, kafkaConnectClient::createCustomPlugin))
                .thenThrow(cException);
        when(exceptionTranslator.translateToCfnException(cException, TestData.CUSTOM_PLUGIN_NAME))
            .thenReturn(cfnException);
        final CfnAlreadyExistsException exception =
            assertThrows(
                CfnAlreadyExistsException.class,
                () -> handler.handleRequest(
                    proxy,
                    TestData.getResourceHandlerRequest(resourceModel),
                    new CallbackContext(),
                    proxyClient,
                    logger));

        assertThat(exception).isEqualTo(cfnException);
    }

    public void handleRequest_afterNDescribeCustomPlugins_success() {
        final ResourceModel resourceModel = TestData.getResourceModel();
        when(translator.translateToCreateRequest(resourceModel, TagHelper.convertToMap(resourceModel.getTags())))
            .thenReturn(TestData.CREATE_CUSTOM_PLUGIN_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.CREATE_CUSTOM_PLUGIN_REQUEST, kafkaConnectClient::createCustomPlugin))
                .thenReturn(TestData.CREATE_CUSTOM_PLUGIN_RESPONSE);
        when(translator.translateToReadRequest(TestData.RESOURCE_MODEL_WITH_ARN))
            .thenReturn(TestData.DESCRIBE_CUSTOM_PLUGIN_REQUEST);
        final DescribeCustomPluginResponse describeRunningCustomPluginResponse =
            TestData.describeResponseWithState(CustomPluginState.ACTIVE);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DESCRIBE_CUSTOM_PLUGIN_REQUEST, kafkaConnectClient::describeCustomPlugin))
                .thenReturn(TestData.describeResponseWithState(CustomPluginState.CREATING))
                .thenReturn(TestData.describeResponseWithState(CustomPluginState.CREATING))
                .thenReturn(describeRunningCustomPluginResponse);
        when(translator.translateFromReadResponse(describeRunningCustomPluginResponse))
            .thenReturn(TestData.RESOURCE_MODEL_WITH_ARN);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.LIST_TAGS_FOR_RESOURCE_REQUEST, kafkaConnectClient::listTagsForResource))
                .thenReturn(TestData.LIST_TAGS_FOR_RESOURCE_RESPONSE);

        final ResourceHandlerRequest<ResourceModel> request =
            TestData.getResourceHandlerRequest(resourceModel);
        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isEqualTo(TestData.DESCRIBE_RESPONSE);
        assertThat(response.getResourceModel().getTags())
            .isEqualTo(request.getDesiredResourceState().getTags());
    }

    @Test
    public void handleRequest_throwsGeneralServiceException_whenDescribeCustomPluginThrowsException() {
        final AwsServiceException cException =
            AwsServiceException.builder().message(TestData.EXCEPTION_MESSAGE).build();
        when(translator.translateToCreateRequest(TestData.getResourceModel(),
            TagHelper.convertToMap(TestData.getResourceModel().getTags())))
                .thenReturn(TestData.CREATE_CUSTOM_PLUGIN_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.CREATE_CUSTOM_PLUGIN_REQUEST, kafkaConnectClient::createCustomPlugin))
                .thenReturn(TestData.CREATE_CUSTOM_PLUGIN_RESPONSE);
        when(translator.translateToReadRequest(TestData.RESOURCE_MODEL_WITH_ARN))
            .thenReturn(TestData.DESCRIBE_CUSTOM_PLUGIN_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DESCRIBE_CUSTOM_PLUGIN_REQUEST, kafkaConnectClient::describeCustomPlugin))
                .thenThrow(cException);

        runHandlerAndAssertExceptionThrownWithMessage(
            CfnGeneralServiceException.class,
            "Error occurred during operation 'AWS::KafkaConnect::CustomPlugin create request "
                + "accepted but failed to get state due to: "
                + TestData.EXCEPTION_MESSAGE
                + "'.");
    }

    @Test
    public void handleRequest_throwsGeneralServiceException_whenCustomPluginsStateFails() {
        setupMocksToReturnCustomPluginState(CustomPluginState.CREATE_FAILED);

        runHandlerAndAssertExceptionThrownWithMessage(
            CfnGeneralServiceException.class,
            "Error occurred during operation 'Couldn't create AWS::KafkaConnect::CustomPlugin "
                + "due to create failure'.");
    }

    @Test
    public void handleRequest_throwsResourceConflictException_whenCustomPluginStartsDeletingDuringCreate() {
        setupMocksToReturnCustomPluginState(CustomPluginState.DELETING);

        runHandlerAndAssertExceptionThrownWithMessage(
            CfnResourceConflictException.class,
            "Resource of type 'AWS::KafkaConnect::CustomPlugin' with identifier '"
                + TestData.CUSTOM_PLUGIN_ARN
                + "' has a conflict. Reason: Another process is deleting this AWS::KafkaConnect::CustomPlugin.");
    }

    @Test
    public void handleRequest_throwsGeneralServiceException_whenCustomPluginReturnsUnexpectedState() {
        setupMocksToReturnCustomPluginState(CustomPluginState.UNKNOWN_TO_SDK_VERSION);

        runHandlerAndAssertExceptionThrownWithMessage(
            CfnGeneralServiceException.class,
            "Error occurred during operation 'AWS::KafkaConnect::CustomPlugin create request accepted "
                + "but current state is unknown'.");
    }

    private void setupMocksToReturnCustomPluginState(final CustomPluginState customPluginState) {
        when(translator.translateToCreateRequest(TestData.getResourceModel(),
            TagHelper.convertToMap(TestData.getResourceModel().getTags())))
                .thenReturn(TestData.CREATE_CUSTOM_PLUGIN_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.CREATE_CUSTOM_PLUGIN_REQUEST, kafkaConnectClient::createCustomPlugin))
                .thenReturn(TestData.CREATE_CUSTOM_PLUGIN_RESPONSE);
        when(translator.translateToReadRequest(TestData.RESOURCE_MODEL_WITH_ARN))
            .thenReturn(TestData.DESCRIBE_CUSTOM_PLUGIN_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DESCRIBE_CUSTOM_PLUGIN_REQUEST, kafkaConnectClient::describeCustomPlugin))
                .thenReturn(TestData.describeResponseWithState(customPluginState));
    }

    private void runHandlerAndAssertExceptionThrownWithMessage(
        final Class<? extends Exception> expectedExceptionClass, final String expectedMessage) {

        final Exception exception =
            assertThrows(
                expectedExceptionClass,
                () -> handler.handleRequest(
                    proxy,
                    TestData.getResourceHandlerRequest(TestData.getResourceModel()),
                    new CallbackContext(),
                    proxyClient,
                    logger));

        assertThat(exception.getMessage()).isEqualTo(expectedMessage);
    }

    private static class TestData {
        private static final String CUSTOM_PLUGIN_NAME = "unit-test-custom-plugin";
        private static final String CUSTOM_PLUGIN_DESCRIPTION =
            "Unit testing custom plugin description";

        private static final long CUSTOM_PLUGIN_REVISION = 1L;
        private static final String CUSTOM_PLUGIN_S3_FILE_KEY = "file-key";

        private static final String CUSTOM_PLUGIN_PROPERTIES_FILE_CONTENT = "propertiesFileContent";

        private static final String CUSTOM_PLUGIN_ARN =
            "arn:aws:kafkaconnect:us-east-1:1111111111:custom-plugin/unit-test-custom-plugin";
        private static final Instant CUSTOM_PLUGIN_CREATION_TIME =
            OffsetDateTime.parse("2021-03-04T14:03:40.818Z").toInstant();
        private static final String CUSTOM_PLUGIN_LOCATION_BUCKET_ARN = "arn:aws:s3:::unit-test-bucket";
        private static final String CUSTOM_PLUGIN_LOCATION_FILE_KEY = "unit-test-file-key.zip";
        private static final String CUSTOM_PLUGIN_LOCATION_OBJECT_VERSION = "1";
        private static final String CUSTOM_PLUGIN_STATE_CODE = "custom-plugin-state-code";
        private static final String CUSTOM_PLUGIN_STATE_DESCRIPTION = "custom-plugin-state-description";
        private static final String CUSTOM_PLUGIN_S3_OBJECT_VERSION = "object-version";
        private static final String CUSTOM_PLUGIN_S3_BUCKET_ARN = "bucket-arn";
        private static final String CUSTOM_PLUGIN_FILE_MD5 = "abcd1234";
        private static final long CUSTOM_PLUGIN_FILE_SIZE = 123456L;
        private static final String EXCEPTION_MESSAGE = "Exception message";
        private static final DescribeCustomPluginResponse FULL_DESCRIBE_CUSTOM_PLUGIN_RESPONSE =
            DescribeCustomPluginResponse.builder()
                .creationTime(CUSTOM_PLUGIN_CREATION_TIME)
                .customPluginArn(CUSTOM_PLUGIN_ARN)
                .customPluginState(CustomPluginState.ACTIVE)
                .name(CUSTOM_PLUGIN_NAME)
                .description(CUSTOM_PLUGIN_DESCRIPTION)
                .stateDescription(
                    StateDescription.builder()
                        .code(CUSTOM_PLUGIN_STATE_CODE)
                        .message(CUSTOM_PLUGIN_STATE_DESCRIPTION)
                        .build())
                .latestRevision(
                    CustomPluginRevisionSummary.builder()
                        .contentType(CustomPluginContentType.ZIP)
                        .creationTime(CUSTOM_PLUGIN_CREATION_TIME)
                        .description(CUSTOM_PLUGIN_DESCRIPTION)
                        .fileDescription(
                            software.amazon.awssdk.services.kafkaconnect.model.CustomPluginFileDescription.builder()
                                .fileMd5(CUSTOM_PLUGIN_FILE_MD5)
                                .fileSize(CUSTOM_PLUGIN_FILE_SIZE)
                                .build())
                        .location(
                            CustomPluginLocationDescription.builder()
                                .s3Location(
                                    S3LocationDescription.builder()
                                        .bucketArn(CUSTOM_PLUGIN_S3_BUCKET_ARN)
                                        .fileKey(CUSTOM_PLUGIN_S3_FILE_KEY)
                                        .objectVersion(CUSTOM_PLUGIN_S3_OBJECT_VERSION)
                                        .build())
                                .build())
                        .revision(CUSTOM_PLUGIN_REVISION)
                        .build())
                .build();
        private static final CustomPluginLocation CUSTOM_PLUGIN_LOCATION =
            CustomPluginLocation.builder()
                .s3Location(
                    S3Location.builder()
                        .bucketArn(CUSTOM_PLUGIN_LOCATION_BUCKET_ARN)
                        .fileKey(CUSTOM_PLUGIN_LOCATION_FILE_KEY)
                        .objectVersion(CUSTOM_PLUGIN_LOCATION_OBJECT_VERSION)
                        .build())
                .build();

        private static final CreateCustomPluginRequest CREATE_CUSTOM_PLUGIN_REQUEST =
            CreateCustomPluginRequest.builder()
                .contentType(CUSTOM_PLUGIN_PROPERTIES_FILE_CONTENT)
                .description(CUSTOM_PLUGIN_DESCRIPTION)
                .location(CUSTOM_PLUGIN_LOCATION)
                .name(CUSTOM_PLUGIN_NAME)
                .build();

        private static final CreateCustomPluginResponse CREATE_CUSTOM_PLUGIN_RESPONSE =
            CreateCustomPluginResponse.builder()
                .customPluginArn(CUSTOM_PLUGIN_ARN)
                .customPluginState(CustomPluginState.ACTIVE)
                .name(CUSTOM_PLUGIN_NAME)
                .revision(CUSTOM_PLUGIN_REVISION)
                .build();

        private static final DescribeCustomPluginRequest DESCRIBE_CUSTOM_PLUGIN_REQUEST =
            DescribeCustomPluginRequest.builder().customPluginArn(CUSTOM_PLUGIN_ARN).build();

        private static final ResourceModel RESOURCE_MODEL_WITH_ARN =
            ResourceModel.builder()
                .name(CUSTOM_PLUGIN_NAME)
                .customPluginArn(CUSTOM_PLUGIN_ARN)
                .tags(TagHelper.convertToList(TAGS))
                .build();

        private static DescribeCustomPluginResponse describeResponseWithState(
            final CustomPluginState state) {
            return DescribeCustomPluginResponse.builder()
                .customPluginArn(CUSTOM_PLUGIN_ARN)
                .name(CUSTOM_PLUGIN_NAME)
                .customPluginState(state)
                .build();
        }

        private static final ProgressEvent<ResourceModel, CallbackContext> DESCRIBE_RESPONSE =
            ProgressEvent.<ResourceModel, CallbackContext>builder()
                .resourceModel(RESOURCE_MODEL_WITH_ARN)
                .status(OperationStatus.SUCCESS)
                .build();

        private static ResourceHandlerRequest<ResourceModel> getResourceHandlerRequest(
            final ResourceModel resourceModel) {

            return ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(resourceModel)
                .desiredResourceTags(TAGS)
                .build();
        }

        private static ResourceModel getResourceModel() {
            return ResourceModel.builder()
                .name(CUSTOM_PLUGIN_NAME)
                .tags(TagHelper.convertToList(TAGS))
                .build();
        }

        private static final ListTagsForResourceRequest LIST_TAGS_FOR_RESOURCE_REQUEST =
            ListTagsForResourceRequest.builder().resourceArn(CUSTOM_PLUGIN_ARN).build();

        private static final ListTagsForResourceResponse LIST_TAGS_FOR_RESOURCE_RESPONSE =
            ListTagsForResourceResponse.builder().tags(TAGS).build();
    }
}
