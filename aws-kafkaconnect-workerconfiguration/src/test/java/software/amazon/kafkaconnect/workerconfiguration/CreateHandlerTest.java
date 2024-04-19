package software.amazon.kafkaconnect.workerconfiguration;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;

import software.amazon.awssdk.services.kafkaconnect.KafkaConnectClient;
import software.amazon.cloudformation.exceptions.CfnAlreadyExistsException;
import software.amazon.awssdk.services.kafkaconnect.model.CreateWorkerConfigurationRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeWorkerConfigurationRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeWorkerConfigurationResponse;
import software.amazon.awssdk.services.kafkaconnect.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.kafkaconnect.model.CreateWorkerConfigurationResponse;
import software.amazon.awssdk.services.kafkaconnect.model.ConflictException;
import software.amazon.awssdk.services.kafkaconnect.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.kafkaconnect.model.WorkerConfigurationRevisionSummary;
import software.amazon.awssdk.services.kafkaconnect.model.WorkerConfigurationRevisionDescription;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

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
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS,
            () -> Duration.ofSeconds(600).toMillis());
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
            .thenReturn(TestData.CREATE_WORKER_CONFIGURATION_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.CREATE_WORKER_CONFIGURATION_REQUEST, kafkaConnectClient::createWorkerConfiguration))
                .thenReturn(TestData.CREATE_WORKER_CONFIGURATION_RESPONSE);
        when(translator.translateToReadRequest(TestData.RESOURCE_MODEL_WITH_ARN))
            .thenReturn(TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST);
        final DescribeWorkerConfigurationResponse describeWorkerConfigurationResponse =
            TestData.describeResponse();
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST, kafkaConnectClient::describeWorkerConfiguration))
                .thenReturn(describeWorkerConfigurationResponse);
        when(translator.translateFromReadResponse(describeWorkerConfigurationResponse))
            .thenReturn(TestData.RESOURCE_MODEL_WITH_ARN);
        when(proxyClient.injectCredentialsAndInvokeV2(TestData.LIST_TAGS_FOR_RESOURCE_REQUEST,
            kafkaConnectClient::listTagsForResource)).thenReturn(TestData.LIST_TAGS_FOR_RESOURCE_RESPONSE);

        final ResourceHandlerRequest<ResourceModel> request = TestData.getResourceHandlerRequest(resourceModel);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(
            proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isEqualTo(TestData.DESCRIBE_RESPONSE);
        assertThat(response.getResourceModel().getTags())
            .isEqualTo(request.getDesiredResourceState().getTags());
    }

    @Test
    public void handleRequest_throwsAlreadyExistsException_whenWorkerConfigurationExists() {
        final ResourceModel resourceModel = TestData.getResourceModel();
        final ConflictException cException = ConflictException.builder().build();
        final CfnAlreadyExistsException cfnException = new CfnAlreadyExistsException(cException);
        when(translator.translateToCreateRequest(resourceModel, TagHelper.convertToMap(resourceModel.getTags())))
            .thenReturn(TestData.CREATE_WORKER_CONFIGURATION_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.CREATE_WORKER_CONFIGURATION_REQUEST,
            kafkaConnectClient::createWorkerConfiguration)).thenThrow(cException);
        when(exceptionTranslator.translateToCfnException(cException, TestData.WORKER_CONFIGURATION_NAME))
            .thenReturn(cfnException);

        final CfnAlreadyExistsException exception = assertThrows(CfnAlreadyExistsException.class,
            () -> handler.handleRequest(proxy, TestData.getResourceHandlerRequest(resourceModel),
                new CallbackContext(), proxyClient, logger));

        assertThat(exception).isEqualTo(cfnException);
    }

    private static class TestData {
        private static final String WORKER_CONFIGURATION_NAME = "unit-test-worker-configuration";
        private static final String WORKER_CONFIGURATION_DESCRIPTION = "Unit testing worker configuration description";

        private static final long WORKER_CONFIGURATION_REVISION = 1L;

        private static final String WORKER_CONFIGURATION_PROPERTIES_FILE_CONTENT = "propertiesFileContent";

        private static final String WORKER_CONFIGURATION_ARN =
            "arn:aws:kafkaconnect:us-east-1:1111111111:worker-configuration/unit-test-worker-configuration";
        private static final Instant WORKER_CONFIGURATION_CREATION_TIME =
            OffsetDateTime.parse("2021-03-04T14:03:40.818Z").toInstant();
        private static final Instant WORKER_CONFIGURATION_REVISION_CREATION_TIME =
            OffsetDateTime.parse("2021-03-04T14:03:40.818Z").toInstant();
        private static final CreateWorkerConfigurationRequest CREATE_WORKER_CONFIGURATION_REQUEST =
            CreateWorkerConfigurationRequest.builder()
                .name(WORKER_CONFIGURATION_NAME)
                .description(WORKER_CONFIGURATION_DESCRIPTION)
                .propertiesFileContent(WORKER_CONFIGURATION_PROPERTIES_FILE_CONTENT)
                .tags(TAGS)
                .build();

        private static final CreateWorkerConfigurationResponse CREATE_WORKER_CONFIGURATION_RESPONSE =
            CreateWorkerConfigurationResponse
                .builder()
                .workerConfigurationArn(WORKER_CONFIGURATION_ARN)
                .creationTime(WORKER_CONFIGURATION_CREATION_TIME)
                .name(WORKER_CONFIGURATION_NAME)
                .latestRevision(workerConfigurationRevisionSummary())
                .build();

        private static final DescribeWorkerConfigurationRequest DESCRIBE_WORKER_CONFIGURATION_REQUEST =
            DescribeWorkerConfigurationRequest.builder()
                .workerConfigurationArn(WORKER_CONFIGURATION_ARN)
                .build();

        private static final ResourceModel RESOURCE_MODEL_WITH_ARN = ResourceModel.builder()
            .name(WORKER_CONFIGURATION_NAME)
            .workerConfigurationArn(WORKER_CONFIGURATION_ARN)
            .tags(TagHelper.convertToSet(TAGS))
            .build();

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
            return ResourceModel
                .builder()
                .name(WORKER_CONFIGURATION_NAME)
                .tags(TagHelper.convertToSet(TAGS))
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

        private static WorkerConfigurationRevisionSummary workerConfigurationRevisionSummary() {
            return WorkerConfigurationRevisionSummary.builder()
                .revision(WORKER_CONFIGURATION_REVISION)
                .description(WORKER_CONFIGURATION_DESCRIPTION)
                .creationTime(WORKER_CONFIGURATION_REVISION_CREATION_TIME)
                .build();
        }

        private static WorkerConfigurationRevisionDescription workerConfigurationRevisionDescription() {
            return WorkerConfigurationRevisionDescription.builder()
                .revision(WORKER_CONFIGURATION_REVISION)
                .description(WORKER_CONFIGURATION_DESCRIPTION)
                .propertiesFileContent(WORKER_CONFIGURATION_PROPERTIES_FILE_CONTENT)
                .build();
        }

        private static final ListTagsForResourceRequest LIST_TAGS_FOR_RESOURCE_REQUEST =
            ListTagsForResourceRequest.builder()
                .resourceArn(WORKER_CONFIGURATION_ARN)
                .build();

        private static final ListTagsForResourceResponse LIST_TAGS_FOR_RESOURCE_RESPONSE =
            ListTagsForResourceResponse
                .builder()
                .tags(TAGS)
                .build();
    }
}
