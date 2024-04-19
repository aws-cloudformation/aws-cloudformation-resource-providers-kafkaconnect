package software.amazon.kafkaconnect.workerconfiguration;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.junit.jupiter.api.AfterEach;
import software.amazon.awssdk.services.kafkaconnect.KafkaConnectClient;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeWorkerConfigurationRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeWorkerConfigurationResponse;
import software.amazon.awssdk.services.kafkaconnect.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.kafkaconnect.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.kafkaconnect.model.NotFoundException;
import software.amazon.awssdk.services.kafkaconnect.model.TagResourceRequest;
import software.amazon.awssdk.services.kafkaconnect.model.TagResourceResponse;
import software.amazon.awssdk.services.kafkaconnect.model.UntagResourceRequest;
import software.amazon.awssdk.services.kafkaconnect.model.UntagResourceResponse;
import software.amazon.awssdk.services.kafkaconnect.model.WorkerConfigurationRevisionDescription;

import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.exceptions.CfnNotUpdatableException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest.ResourceHandlerRequestBuilder;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.never;

@ExtendWith(MockitoExtension.class)
public class UpdateHandlerTest extends AbstractTestBase {

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

    private ReadHandler readHandler;

    private UpdateHandler handler;

    @BeforeEach
    public void setup() {
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS,
            () -> Duration.ofSeconds(600).toMillis());
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
    public void handleRequest_SimpleSuccess() {
        final ResourceModel model = TestData.RESOURCE_MODEL.toBuilder().build();

        final ResourceHandlerRequest<ResourceModel> request =
            TestData.createResourceHandlerRequestWithoutSystemTags(Objects.requireNonNull(model),
                Objects.requireNonNull(model));
        final DescribeWorkerConfigurationResponse describeWorkerConfigurationResponse =
            TestData.describeResponse();

        when(translator.translateToReadRequest(TestData.RESOURCE_MODEL))
            .thenReturn(TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST, kafkaConnectClient::describeWorkerConfiguration))
                .thenReturn(describeWorkerConfigurationResponse);
        when(translator.translateFromReadResponse(describeWorkerConfigurationResponse))
            .thenReturn(model);
        when(proxyClient.injectCredentialsAndInvokeV2(TestData.LIST_TAGS_FOR_RESOURCE_REQUEST,
            kafkaConnectClient::listTagsForResource)).thenReturn(TestData.LIST_TAGS_FOR_RESOURCE_RESPONSE);

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel().getWorkerConfigurationArn())
            .isEqualTo(TestData.WORKER_CONFIGURATION_ARN);
        assertThat(response.getResourceModel().getName()).isEqualTo(TestData.WORKER_CONFIGURATION_NAME);
        assertThat(response.getResourceModel().getDescription()).isEqualTo(TestData.WORKER_CONFIGURATION_DESCRIPTION);
        assertThat(response.getResourceModel().getRevision()).isEqualTo(TestData.WORKER_CONFIGURATION_REVISION);
        assertThat(response.getResourceModel().getPropertiesFileContent())
            .isEqualTo(TestData.WORKER_CONFIGURATION_PROPERTIES_FILE_CONTENT);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(2))
            .describeWorkerConfiguration(any(DescribeWorkerConfigurationRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_FailsWith_CfnNotUpdatableException_NameChange() {
        final ResourceModel model = ResourceModel.builder()
            .workerConfigurationArn(TestData.WORKER_CONFIGURATION_ARN)
            .name(TestData.WORKER_CONFIGURATION_NAME_1)
            .description(TestData.WORKER_CONFIGURATION_DESCRIPTION)
            .revision(TestData.WORKER_CONFIGURATION_REVISION)
            .propertiesFileContent(TestData.WORKER_CONFIGURATION_PROPERTIES_FILE_CONTENT)
            .build();

        final ResourceHandlerRequest<ResourceModel> request =
            TestData.createResourceHandlerRequest(Objects.requireNonNull(TestData.RESOURCE_MODEL), model);

        final DescribeWorkerConfigurationResponse describeWorkerConfigurationResponse =
            TestData.describeResponse();

        when(translator.translateToReadRequest(TestData.RESOURCE_MODEL))
            .thenReturn(TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST, kafkaConnectClient::describeWorkerConfiguration))
                .thenReturn(describeWorkerConfigurationResponse);

        assertThrows(CfnNotUpdatableException.class, () -> {
            handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);
        });
    }

    @Test
    public void handleRequest_FailsWith_CfnNotUpdatableException_DescriptionChange() {
        final ResourceModel model = ResourceModel.builder()
            .workerConfigurationArn(TestData.WORKER_CONFIGURATION_ARN)
            .name(TestData.WORKER_CONFIGURATION_NAME)
            .description(TestData.WORKER_CONFIGURATION_DESCRIPTION_1)
            .revision(TestData.WORKER_CONFIGURATION_REVISION)
            .propertiesFileContent(TestData.WORKER_CONFIGURATION_PROPERTIES_FILE_CONTENT)
            .build();

        final ResourceHandlerRequest<ResourceModel> request =
            TestData.createResourceHandlerRequest(Objects.requireNonNull(TestData.RESOURCE_MODEL), model);

        final DescribeWorkerConfigurationResponse describeWorkerConfigurationResponse =
            TestData.describeResponse();

        when(translator.translateToReadRequest(TestData.RESOURCE_MODEL))
            .thenReturn(TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST, kafkaConnectClient::describeWorkerConfiguration))
                .thenReturn(describeWorkerConfigurationResponse);

        assertThrows(CfnNotUpdatableException.class, () -> {
            handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);
        });
    }

    @Test
    public void handleRequest_FailsWith_CfnNotUpdatableException_PropertiesFileContentChange() {
        final ResourceModel model = ResourceModel.builder()
            .workerConfigurationArn(TestData.WORKER_CONFIGURATION_ARN)
            .name(TestData.WORKER_CONFIGURATION_NAME)
            .description(TestData.WORKER_CONFIGURATION_DESCRIPTION)
            .revision(TestData.WORKER_CONFIGURATION_REVISION)
            .propertiesFileContent(TestData.WORKER_CONFIGURATION_PROPERTIES_FILE_CONTENT_1)
            .build();

        final ResourceHandlerRequest<ResourceModel> request =
            TestData.createResourceHandlerRequest(Objects.requireNonNull(TestData.RESOURCE_MODEL), model);

        final DescribeWorkerConfigurationResponse describeWorkerConfigurationResponse =
            TestData.describeResponse();

        when(translator.translateToReadRequest(TestData.RESOURCE_MODEL))
            .thenReturn(TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST, kafkaConnectClient::describeWorkerConfiguration))
                .thenReturn(describeWorkerConfigurationResponse);

        assertThrows(CfnNotUpdatableException.class, () -> {
            handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);
        });
    }

    @Test
    public void handleRequest_FailsWith_CfnNotUpdatableException_RevisionChange() {
        final ResourceModel model = ResourceModel.builder()
            .workerConfigurationArn(TestData.WORKER_CONFIGURATION_ARN)
            .name(TestData.WORKER_CONFIGURATION_NAME)
            .description(TestData.WORKER_CONFIGURATION_DESCRIPTION)
            .revision(TestData.WORKER_CONFIGURATION_REVISION_1)
            .propertiesFileContent(TestData.WORKER_CONFIGURATION_PROPERTIES_FILE_CONTENT)
            .build();

        final ResourceHandlerRequest<ResourceModel> request =
            TestData.createResourceHandlerRequest(Objects.requireNonNull(TestData.RESOURCE_MODEL), model);

        final DescribeWorkerConfigurationResponse describeWorkerConfigurationResponse =
            TestData.describeResponse();

        when(translator.translateToReadRequest(TestData.RESOURCE_MODEL))
            .thenReturn(TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST, kafkaConnectClient::describeWorkerConfiguration))
                .thenReturn(describeWorkerConfigurationResponse);

        assertThrows(CfnNotUpdatableException.class, () -> {
            handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);
        });
    }

    @Test
    public void handleRequest_FailsWith_CfnNotUpdatableException_WorkerConfigurationArnChange() {
        final ResourceModel model = ResourceModel.builder()
            .workerConfigurationArn(TestData.WORKER_CONFIGURATION_ARN_1)
            .name(TestData.WORKER_CONFIGURATION_NAME)
            .description(TestData.WORKER_CONFIGURATION_DESCRIPTION)
            .revision(TestData.WORKER_CONFIGURATION_REVISION)
            .propertiesFileContent(TestData.WORKER_CONFIGURATION_PROPERTIES_FILE_CONTENT)
            .build();

        final ResourceHandlerRequest<ResourceModel> request =
            TestData.createResourceHandlerRequest(Objects.requireNonNull(TestData.RESOURCE_MODEL), model);

        final DescribeWorkerConfigurationResponse describeWorkerConfigurationResponse =
            TestData.describeResponse();

        when(translator.translateToReadRequest(TestData.RESOURCE_MODEL))
            .thenReturn(TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST, kafkaConnectClient::describeWorkerConfiguration))
                .thenReturn(describeWorkerConfigurationResponse);

        assertThrows(CfnNotUpdatableException.class, () -> {
            handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);
        });
    }

    @Test
    public void handleRequest_FailsWith_CfnNotFoundException() {
        final NotFoundException serviceException = NotFoundException.builder().build();
        final CfnNotFoundException cfnException = new CfnNotFoundException(serviceException);

        when(translator.translateToReadRequest(TestData.RESOURCE_MODEL))
            .thenReturn(TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST,
            kafkaConnectClient::describeWorkerConfiguration)).thenThrow(serviceException);
        when(exceptionTranslator.translateToCfnException(serviceException, TestData.WORKER_CONFIGURATION_ARN))
            .thenReturn(cfnException);

        final ResourceHandlerRequest<ResourceModel> request =
            TestData.createResourceHandlerRequest(Objects.requireNonNull(TestData.RESOURCE_MODEL), null);

        final CfnNotFoundException exception = assertThrows(CfnNotFoundException.class, () -> handler
            .handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));

        assertThat(exception).isEqualTo(cfnException);
    }

    @Test
    public void handleRequest_addNewTags() {
        final Set<Tag> tagsSet = new HashSet<>();
        tagsSet.add(Tag.builder().key(TestData.WORKER_CONFIGURATION_TAG_KEY)
            .value(TestData.WORKER_CONFIGURATION_TAG_VALUE).build());

        final Map<String, String> tagsMap = new HashMap<>();
        tagsMap.put(TestData.WORKER_CONFIGURATION_TAG_KEY, TestData.WORKER_CONFIGURATION_TAG_VALUE);

        final ResourceModel model = TestData.RESOURCE_MODEL.toBuilder()
            .tags(tagsSet)
            .build();

        final ResourceHandlerRequest<ResourceModel> request =
            TestData.createResourceHandlerRequest(Objects.requireNonNull(model), TestData.RESOURCE_MODEL);

        final DescribeWorkerConfigurationResponse describeWorkerConfigurationResponse =
            TestData.describeResponse();

        when(translator.translateToReadRequest(model))
            .thenReturn(TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST, kafkaConnectClient::describeWorkerConfiguration))
                .thenReturn(describeWorkerConfigurationResponse);
        when(translator.translateFromReadResponse(describeWorkerConfigurationResponse))
            .thenReturn(model);
        ;
        when(proxyClient.client().tagResource(any(TagResourceRequest.class)))
            .thenReturn(TagResourceResponse.builder().build());
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
            .thenReturn(ListTagsForResourceResponse
                .builder()
                .tags(tagsMap)
                .build());

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(1)).tagResource(any(TagResourceRequest.class));
        verify(proxyClient.client(), never()).untagResource(any(UntagResourceRequest.class));
        verify(proxyClient.client(), times(2))
            .describeWorkerConfiguration(any(DescribeWorkerConfigurationRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_updateTags() {
        final Set<Tag> tagsSet = new HashSet<>();
        tagsSet.add(Tag.builder().key(TestData.WORKER_CONFIGURATION_TAG_KEY)
            .value(TestData.WORKER_CONFIGURATION_TAG_VALUE).build());

        final Set<Tag> prevTagsSet = new HashSet<>();
        prevTagsSet.add(Tag.builder().key(TestData.WORKER_CONFIGURATION_TAG_KEY).value("OLD_VALUE").build());

        final Map<String, String> tagsMap = new HashMap<>();
        tagsMap.put(TestData.WORKER_CONFIGURATION_TAG_KEY, TestData.WORKER_CONFIGURATION_TAG_VALUE);

        final ResourceModel model = TestData.RESOURCE_MODEL.toBuilder()
            .tags(tagsSet)
            .build();

        final ResourceModel prevModel = TestData.RESOURCE_MODEL.toBuilder()
            .tags(prevTagsSet)
            .build();

        final ResourceHandlerRequest<ResourceModel> request =
            TestData.createResourceHandlerRequest(Objects.requireNonNull(model), prevModel);

        final DescribeWorkerConfigurationResponse describeWorkerConfigurationResponse =
            TestData.describeResponse();

        when(translator.translateToReadRequest(model))
            .thenReturn(TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST, kafkaConnectClient::describeWorkerConfiguration))
                .thenReturn(describeWorkerConfigurationResponse);
        when(translator.translateFromReadResponse(describeWorkerConfigurationResponse))
            .thenReturn(model);
        ;
        when(proxyClient.client().tagResource(any(TagResourceRequest.class)))
            .thenReturn(TagResourceResponse.builder().build());
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
            .thenReturn(ListTagsForResourceResponse
                .builder()
                .tags(tagsMap)
                .build());

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel().getName()).isEqualTo(TestData.WORKER_CONFIGURATION_NAME);
        assertThat(response.getResourceModel().getDescription()).isEqualTo(TestData.WORKER_CONFIGURATION_DESCRIPTION);
        assertThat(response.getResourceModel().getPropertiesFileContent())
            .isEqualTo(TestData.WORKER_CONFIGURATION_PROPERTIES_FILE_CONTENT);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(2))
            .describeWorkerConfiguration(any(DescribeWorkerConfigurationRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
        verify(proxyClient.client(), never()).untagResource(any(UntagResourceRequest.class));
        verify(proxyClient.client(), times(1)).tagResource(any(TagResourceRequest.class));
    }

    @Test
    public void handleRequest_RemoveTags() {
        final Set<Tag> tagsSet = new HashSet<>();
        tagsSet.add(Tag.builder().key(TestData.WORKER_CONFIGURATION_TAG_KEY)
            .value(TestData.WORKER_CONFIGURATION_TAG_VALUE).build());

        final ResourceModel model = TestData.RESOURCE_MODEL.toBuilder()
            .build();

        final ResourceModel prevModel = TestData.RESOURCE_MODEL.toBuilder()
            .tags(tagsSet)
            .build();

        final ResourceHandlerRequest<ResourceModel> request =
            TestData.createResourceHandlerRequest(Objects.requireNonNull(model), prevModel);

        final DescribeWorkerConfigurationResponse describeWorkerConfigurationResponse =
            TestData.describeResponse();

        when(translator.translateToReadRequest(model))
            .thenReturn(TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST, kafkaConnectClient::describeWorkerConfiguration))
                .thenReturn(describeWorkerConfigurationResponse);
        when(translator.translateFromReadResponse(describeWorkerConfigurationResponse))
            .thenReturn(model);
        ;
        when(proxyClient.client().untagResource(any(UntagResourceRequest.class)))
            .thenReturn(UntagResourceResponse.builder().build());
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
            .thenReturn(ListTagsForResourceResponse
                .builder()
                .build());

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel().getName()).isEqualTo(TestData.WORKER_CONFIGURATION_NAME);
        assertThat(response.getResourceModel().getDescription()).isEqualTo(TestData.WORKER_CONFIGURATION_DESCRIPTION);
        assertThat(response.getResourceModel().getPropertiesFileContent())
            .isEqualTo(TestData.WORKER_CONFIGURATION_PROPERTIES_FILE_CONTENT);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(2))
            .describeWorkerConfiguration(any(DescribeWorkerConfigurationRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
        verify(proxyClient.client(), times(1)).untagResource(any(UntagResourceRequest.class));
        verify(proxyClient.client(), never()).tagResource(any(TagResourceRequest.class));
    }

    @Test
    public void handleRequest_AddRemoveTags() {
        final String tagKeyRemove = "TEST_KEY_REMOVE";
        final String tagValueRemove = "TEST_VALUE_REMOVE";

        final Set<Tag> tagsSet = new HashSet<>();
        tagsSet.add(Tag.builder().key(TestData.WORKER_CONFIGURATION_TAG_KEY)
            .value(TestData.WORKER_CONFIGURATION_TAG_VALUE).build());

        final Set<Tag> prevTagsSet = new HashSet<>();
        prevTagsSet.add(Tag.builder().key(tagKeyRemove).value(tagValueRemove).build());

        final Map<String, String> tagsMap = new HashMap<>();
        tagsMap.put(TestData.WORKER_CONFIGURATION_TAG_KEY, TestData.WORKER_CONFIGURATION_TAG_VALUE);

        final ResourceModel model = TestData.RESOURCE_MODEL.toBuilder()
            .tags(tagsSet)
            .build();

        final ResourceModel prevModel = TestData.RESOURCE_MODEL.toBuilder()
            .tags(prevTagsSet)
            .build();

        final ResourceHandlerRequest<ResourceModel> request =
            TestData.createResourceHandlerRequest(Objects.requireNonNull(model), prevModel);

        final DescribeWorkerConfigurationResponse describeWorkerConfigurationResponse =
            TestData.describeResponse();

        when(translator.translateToReadRequest(model))
            .thenReturn(TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST, kafkaConnectClient::describeWorkerConfiguration))
                .thenReturn(describeWorkerConfigurationResponse);
        when(translator.translateFromReadResponse(describeWorkerConfigurationResponse))
            .thenReturn(model);
        ;
        when(proxyClient.client().tagResource(any(TagResourceRequest.class)))
            .thenReturn(TagResourceResponse.builder().build());
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
            .thenReturn(ListTagsForResourceResponse
                .builder()
                .tags(tagsMap)
                .build());
        when(proxyClient.client().untagResource(any(UntagResourceRequest.class)))
            .thenReturn(UntagResourceResponse.builder().build());

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel().getName()).isEqualTo(TestData.WORKER_CONFIGURATION_NAME);
        assertThat(response.getResourceModel().getDescription()).isEqualTo(TestData.WORKER_CONFIGURATION_DESCRIPTION);
        assertThat(response.getResourceModel().getPropertiesFileContent())
            .isEqualTo(TestData.WORKER_CONFIGURATION_PROPERTIES_FILE_CONTENT);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(2))
            .describeWorkerConfiguration(any(DescribeWorkerConfigurationRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
        verify(proxyClient.client(), times(1)).untagResource(any(UntagResourceRequest.class));
        verify(proxyClient.client(), times(1)).tagResource(any(TagResourceRequest.class));
    }

    private static class TestData {
        private static final String WORKER_CONFIGURATION_NAME = "unit-test-worker-configuration";

        private static final String WORKER_CONFIGURATION_NAME_1 = "unit-test-worker-configuration-1";

        private static final String WORKER_CONFIGURATION_DESCRIPTION = "Unit testing worker configuration description";

        private static final String WORKER_CONFIGURATION_DESCRIPTION_1 =
            "Unit testing worker configuration description 1";

        private static final long WORKER_CONFIGURATION_REVISION = 1L;

        private static final long WORKER_CONFIGURATION_REVISION_1 = 2L;

        private static final String WORKER_CONFIGURATION_PROPERTIES_FILE_CONTENT = "propertiesFileContent";

        private static final String WORKER_CONFIGURATION_PROPERTIES_FILE_CONTENT_1 = "propertiesFileContent1";

        private static final String WORKER_CONFIGURATION_ARN =
            "arn:aws:kafkaconnect:us-east-1:1111111111:worker-configuration/unit-test-worker-configuration";

        private static final String WORKER_CONFIGURATION_ARN_1 =
            "arn:aws:kafkaconnect:us-east-1:1111111111:worker-configuration/unit-test-worker-configuration-1";
        private static final Instant WORKER_CONFIGURATION_CREATION_TIME =
            OffsetDateTime.parse("2021-03-04T14:03:40.818Z").toInstant();
        private static final String WORKER_CONFIGURATION_TAG_KEY = "unit-test-key";
        private static final String WORKER_CONFIGURATION_TAG_VALUE = "unit-test-value";

        private static final Map<String, String> SYSTEM_TAGS = new HashMap<String, String>() {
            {
                put("SYSTEM_TAG_TEST1", "SYSTEM_TAG_TEST_VALUE1");
                put("SYSTEM_TAG_TEST2", "SYSTEM_TAG_TEST_VALUE2");
            }
        };

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

        private static final ListTagsForResourceRequest LIST_TAGS_FOR_RESOURCE_REQUEST =
            ListTagsForResourceRequest.builder()
                .resourceArn(WORKER_CONFIGURATION_ARN)
                .build();

        private static final ListTagsForResourceResponse LIST_TAGS_FOR_RESOURCE_RESPONSE =
            ListTagsForResourceResponse
                .builder()
                .tags(TAGS)
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
                .build();
        }

        private static ResourceHandlerRequest<ResourceModel> createResourceHandlerRequestWithoutSystemTags(
            @Nonnull ResourceModel desiredModel, @Nullable ResourceModel previousModel) {
            final ResourceHandlerRequestBuilder<ResourceModel> requestBuilder =
                ResourceHandlerRequest.<ResourceModel>builder()
                    .desiredResourceState(desiredModel)
                    .desiredResourceTags(TagHelper.convertToMap(desiredModel.getTags()));

            if (previousModel != null) {
                requestBuilder.previousResourceState(previousModel)
                    .previousResourceTags(TagHelper.convertToMap(previousModel.getTags()));
            }

            return requestBuilder.build();
        }

        private static ResourceHandlerRequest<ResourceModel> createResourceHandlerRequest(
            @Nonnull ResourceModel desiredModel, @Nullable ResourceModel previousModel) {
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
