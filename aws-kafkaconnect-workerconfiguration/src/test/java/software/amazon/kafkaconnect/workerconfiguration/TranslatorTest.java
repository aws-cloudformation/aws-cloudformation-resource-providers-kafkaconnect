package software.amazon.kafkaconnect.workerconfiguration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.kafkaconnect.model.CreateWorkerConfigurationRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeWorkerConfigurationRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeWorkerConfigurationResponse;
import software.amazon.awssdk.services.kafkaconnect.model.ListWorkerConfigurationsRequest;
import software.amazon.awssdk.services.kafkaconnect.model.ListWorkerConfigurationsResponse;
import software.amazon.awssdk.services.kafkaconnect.model.WorkerConfigurationSummary;
import software.amazon.awssdk.services.kafkaconnect.model.WorkerConfigurationRevisionSummary;
import software.amazon.awssdk.services.kafkaconnect.model.WorkerConfigurationRevisionDescription;
import software.amazon.awssdk.services.kafkaconnect.model.TagResourceRequest;
import software.amazon.awssdk.services.kafkaconnect.model.UntagResourceRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DeleteWorkerConfigurationRequest;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.*;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
public class TranslatorTest extends AbstractTestBase {

    private final Translator translator = new Translator();

    @Test
    public void translateToCreateRequest_success() {
        compareCreateRequest(
            translator.translateToCreateRequest(TestData.RESOURCE_CREATE_MODEL,
                TagHelper.convertToMap(TestData.RESOURCE_CREATE_MODEL.getTags())),
            TestData.CREATE_WORKER_CONFIGURATION_REQUEST);
    }

    @Test
    public void translateToReadRequest_success() {
        assertThat(translator.translateToReadRequest(TestData.READ_REQUEST_RESOURCE_MODEL))
            .isEqualTo(TestData.DESCRIBE_WORKER_CONFIGURATION_REQUEST);
    }

    @Test
    public void translateFromReadResponse_success() {
        assertThat(translator.translateFromReadResponse(TestData.DESCRIBE_WORKER_CONFIGURATION_RESPONSE))
            .isEqualTo(TestData.READ_RESPONSE_DESCRIBE_MODEL);
    }

    @Test
    public void translateToListRequest_success() {
        assertThat(translator.translateToListRequest(TestData.NEXT_TOKEN))
            .isEqualTo(TestData.LIST_WORKER_CONFIGURATIONS_REQUEST);
    }

    @Test
    public void translateFromListResponse_success() {
        assertThat(translator.translateFromListResponse(TestData.LIST_WORKER_CONFIGURATIONS_RESPONSE))
            .isEqualTo(TestData.LIST_WORKER_CONFIGURATION_MODELS);
    }

    @Test
    public void translateToTagResourceRequest_success() {
        assertThat(Translator.tagResourceRequest(TestData.TAG_RESOURCE_REQUEST_RESOURCE_MODEL, TAGS))
            .isEqualTo(TestData.TAG_RESOURCE_REQUEST);
    }

    @Test
    public void translateToUntagResourceRequest_success() {
        Set<String> tagsToUntag = new HashSet<>();
        tagsToUntag.add(TestData.TAG_KEY);
        assertThat(Translator.untagResourceRequest(TestData.UNTAG_RESOURCE_REQUEST_RESOURCE_MODEL, tagsToUntag))
            .isEqualTo(TestData.UNTAG_RESOURCE_REQUEST);
    }

    @Test
    public void translateToDeleteRequest_success() {
        assertThat(translator.translateToDeleteRequest(TestData.DELETE_REQUEST_RESOURCE_MODEL))
            .isEqualTo(TestData.DELETE_WORKER_CONFIGURATION_REQUEST);
    }

    private void compareCreateRequest(final CreateWorkerConfigurationRequest request1,
        final CreateWorkerConfigurationRequest request2) {
        // compare all fields without arrays
        assertThat(request1.name()).isEqualTo(request2.name());
        assertThat(request1.description()).isEqualTo(request2.description());
        assertThat(request1.propertiesFileContent()).isEqualTo(request2.propertiesFileContent());
    }

    private static class TestData {

        private static final String WORKER_CONFIGURATION_NAME_1 = "unit-test-worker-configuration-1";

        private static final String WORKER_CONFIGURATION_NAME_2 = "unit-test-worker-configuration-2";

        private static final String WORKER_CONFIGURATION_DESCRIPTION = "Unit testing worker configuration description";

        private static final long WORKER_CONFIGURATION_REVISION = 1L;

        private static final String WORKER_CONFIGURATION_PROPERTIES_FILE_CONTENT = "propertiesFileContent";

        private static final String WORKER_CONFIGURATION_ARN_1 =
            "arn:aws:kafkaconnect:us-east-1:1111111111:worker-configuration/unit-test-worker-configuration-1";

        private static final String WORKER_CONFIGURATION_ARN_2 =
            "arn:aws:kafkaconnect:us-east-1:1111111111:worker-configuration/unit-test-worker-configuration-2";

        private static final Instant WORKER_CONFIGURATION_CREATION_TIME =
            OffsetDateTime.parse("2021-03-04T14:03:40.818Z").toInstant();

        private static final Instant WORKER_CONFIGURATION_REVISION_CREATION_TIME =
            OffsetDateTime.parse("2021-03-04T14:03:40.818Z").toInstant();

        private static final String NEXT_TOKEN = "1234abcd";

        private static final String TAG_KEY = "key";

        private static final ResourceModel READ_REQUEST_RESOURCE_MODEL =
            ResourceModel.builder()
                .workerConfigurationArn(WORKER_CONFIGURATION_ARN_1)
                .build();

        private static final ResourceModel DELETE_REQUEST_RESOURCE_MODEL =
            ResourceModel.builder()
                .workerConfigurationArn(WORKER_CONFIGURATION_ARN_1)
                .build();

        private static final ResourceModel TAG_RESOURCE_REQUEST_RESOURCE_MODEL =
            ResourceModel.builder()
                .workerConfigurationArn(WORKER_CONFIGURATION_ARN_1)
                .build();

        private static final ResourceModel UNTAG_RESOURCE_REQUEST_RESOURCE_MODEL =
            ResourceModel.builder()
                .workerConfigurationArn(WORKER_CONFIGURATION_ARN_1)
                .build();

        private static final ResourceModel READ_RESPONSE_DESCRIBE_MODEL =
            ResourceModel.builder().name(WORKER_CONFIGURATION_NAME_1).workerConfigurationArn(WORKER_CONFIGURATION_ARN_1)
                .description(WORKER_CONFIGURATION_DESCRIPTION)
                .propertiesFileContent(workerConfigurationRevisionDescription().propertiesFileContent())
                .revision(workerConfigurationRevisionDescription().revision()).build();

        private static final DescribeWorkerConfigurationRequest DESCRIBE_WORKER_CONFIGURATION_REQUEST =
            DescribeWorkerConfigurationRequest.builder()
                .workerConfigurationArn(WORKER_CONFIGURATION_ARN_1)
                .build();

        private static final DescribeWorkerConfigurationResponse DESCRIBE_WORKER_CONFIGURATION_RESPONSE =
            DescribeWorkerConfigurationResponse
                .builder()
                .workerConfigurationArn(WORKER_CONFIGURATION_ARN_1)
                .creationTime(WORKER_CONFIGURATION_CREATION_TIME)
                .name(WORKER_CONFIGURATION_NAME_1)
                .description(WORKER_CONFIGURATION_DESCRIPTION)
                .latestRevision(workerConfigurationRevisionDescription())
                .build();

        private static final ListWorkerConfigurationsRequest LIST_WORKER_CONFIGURATIONS_REQUEST =
            ListWorkerConfigurationsRequest.builder()
                .nextToken(NEXT_TOKEN)
                .build();

        private static final List<ResourceModel> LIST_WORKER_CONFIGURATION_MODELS = asList(
            ResourceModel.builder().workerConfigurationArn(WORKER_CONFIGURATION_ARN_1).build(),
            ResourceModel.builder().workerConfigurationArn(WORKER_CONFIGURATION_ARN_2).build());

        private static final ListWorkerConfigurationsResponse LIST_WORKER_CONFIGURATIONS_RESPONSE =
            ListWorkerConfigurationsResponse.builder()
                .workerConfigurations(
                    asList(fullWorkerConfigurationSummary(WORKER_CONFIGURATION_NAME_1, WORKER_CONFIGURATION_ARN_1),
                        fullWorkerConfigurationSummary(WORKER_CONFIGURATION_NAME_2, WORKER_CONFIGURATION_ARN_2)))
                .build();

        private static WorkerConfigurationSummary fullWorkerConfigurationSummary(final String name, final String arn) {
            return WorkerConfigurationSummary.builder()
                .workerConfigurationArn(arn)
                .name(name)
                .creationTime(WORKER_CONFIGURATION_CREATION_TIME)
                .description(WORKER_CONFIGURATION_DESCRIPTION)
                .latestRevision(workerConfigurationRevisionSummary())
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

        private static final ResourceModel RESOURCE_CREATE_MODEL =
            ResourceModel.builder()
                .name(WORKER_CONFIGURATION_NAME_1)
                .description(WORKER_CONFIGURATION_DESCRIPTION)
                .propertiesFileContent(WORKER_CONFIGURATION_PROPERTIES_FILE_CONTENT)
                .build();

        private static final CreateWorkerConfigurationRequest CREATE_WORKER_CONFIGURATION_REQUEST =
            CreateWorkerConfigurationRequest.builder()
                .name(WORKER_CONFIGURATION_NAME_1)
                .description(WORKER_CONFIGURATION_DESCRIPTION)
                .propertiesFileContent(WORKER_CONFIGURATION_PROPERTIES_FILE_CONTENT)
                .build();

        private static final TagResourceRequest TAG_RESOURCE_REQUEST =
            TagResourceRequest.builder()
                .tags(TAGS)
                .resourceArn(WORKER_CONFIGURATION_ARN_1)
                .build();

        private static final UntagResourceRequest UNTAG_RESOURCE_REQUEST =
            UntagResourceRequest.builder()
                .tagKeys(TAG_KEY)
                .resourceArn(WORKER_CONFIGURATION_ARN_1)
                .build();

        private static final DeleteWorkerConfigurationRequest DELETE_WORKER_CONFIGURATION_REQUEST =
            DeleteWorkerConfigurationRequest.builder()
                .workerConfigurationArn(WORKER_CONFIGURATION_ARN_1)
                .build();
    }
}
