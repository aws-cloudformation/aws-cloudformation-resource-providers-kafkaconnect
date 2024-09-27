package software.amazon.kafkaconnect.customplugin;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.kafkaconnect.model.CreateCustomPluginRequest;
import software.amazon.awssdk.services.kafkaconnect.model.CustomPluginContentType;
import software.amazon.awssdk.services.kafkaconnect.model.CustomPluginLocationDescription;
import software.amazon.awssdk.services.kafkaconnect.model.CustomPluginRevisionSummary;
import software.amazon.awssdk.services.kafkaconnect.model.CustomPluginState;
import software.amazon.awssdk.services.kafkaconnect.model.CustomPluginSummary;
import software.amazon.awssdk.services.kafkaconnect.model.DeleteCustomPluginRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeCustomPluginRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeCustomPluginResponse;
import software.amazon.awssdk.services.kafkaconnect.model.ListCustomPluginsRequest;
import software.amazon.awssdk.services.kafkaconnect.model.ListCustomPluginsResponse;
import software.amazon.awssdk.services.kafkaconnect.model.S3LocationDescription;
import software.amazon.awssdk.services.kafkaconnect.model.StateDescription;
import software.amazon.awssdk.services.kafkaconnect.model.TagResourceRequest;
import software.amazon.awssdk.services.kafkaconnect.model.UntagResourceRequest;

@ExtendWith(MockitoExtension.class)
public class TranslatorTest extends AbstractTestBase {

    private Translator translator = new Translator();

    @Test
    public void translateToCreateRequest_success() {
        assertThat(translator.translateToCreateRequest(TestData.CREATE_REQUEST_RESOURCE_MODEL,
            TagHelper.convertToMap(TestData.CREATE_REQUEST_RESOURCE_MODEL.getTags())))
                .isEqualTo(TestData.CREATE_CUSTOM_PLUGIN_REQUEST);
    }

    @Test
    public void translateToReadRequest_success() {
        assertThat(translator.translateToReadRequest(TestData.READ_REQUEST_RESOURCE_MODEL))
            .isEqualTo(TestData.DESCRIBE_CUSTOM_PLUGIN_REQUEST);
    }

    @Test
    public void translateFromReadResponse_fullCustomPlugin_success() {
        assertThat(translator.translateFromReadResponse(TestData.FULL_DESCRIBE_CUSTOM_PLUGIN_RESPONSE))
            .isEqualTo(TestData.FULL_RESOURCE_DESCRIBE_MODEL);
    }

    @Test
    public void translateToListRequest_success() {
        assertThat(translator.translateToListRequest(TestData.NEXT_TOKEN))
            .isEqualTo(TestData.LIST_CUSTOM_PLUGINS_REQUEST);
    }

    @Test
    public void translateFromListResponse_success() {
        assertThat(translator.translateFromListResponse(TestData.LIST_CUSTOM_PLUGINS_RESPONSE))
            .isEqualTo(TestData.LIST_CUSTOM_PLUGIN_MODELS);
    }

    @Test
    public void translateToTagResourceRequest_success() {
        final ResourceModel model = TestData.buildBaseModel(TestData.CUSTOM_PLUGIN_ARN);
        assertThat(Translator.translateToTagRequest(model, TAGS))
            .isEqualTo(TestData.TAG_RESOURCE_REQUEST);
    }

    @Test
    public void translateToUntagResourceRequest_success() {
        final Set<String> tagsToUntag = new HashSet<>();
        tagsToUntag.add(TestData.TAG_KEY_TO_REMOVE);
        final ResourceModel model = TestData.buildBaseModel(TestData.CUSTOM_PLUGIN_ARN);
        assertThat(Translator.translateToUntagRequest(model, tagsToUntag))
            .isEqualTo(TestData.UNTAG_RESOURCE_REQUEST);
    }

    @Test
    public void translateToDeleteRequest_success() {
        assertThat(translator.translateToDeleteRequest(TestData.DELETE_REQUEST_RESOURCE_MODEL))
            .isEqualTo(TestData.DELETE_CUSTOM_PLUGIN_REQUEST);
    }

    @Test
    public void sdkCustomPluginFileDescriptionToResourceCustomPluginFileDescription_success() {
        assertThat(
            Translator.sdkCustomPluginFileDescriptionToResourceCustomPluginFileDescription(
                TestData.FULL_DESCRIBE_CUSTOM_PLUGIN_RESPONSE.latestRevision().fileDescription()))
                    .isEqualTo(TestData.FULL_RESOURCE_DESCRIBE_MODEL.getFileDescription());
        assertThat(
            Translator.sdkCustomPluginFileDescriptionToResourceCustomPluginFileDescription(null))
                .isEqualTo(null);
    }

    @Test
    public void sdkS3LocationDescriptionToResourceS3Location_success() {
        assertThat(
            Translator.sdkS3LocationDescriptionToResourceS3Location(
                TestData.FULL_DESCRIBE_CUSTOM_PLUGIN_RESPONSE.latestRevision().location().s3Location()))
                    .isEqualTo(TestData.FULL_RESOURCE_DESCRIBE_MODEL.getLocation().getS3Location());
        assertThat(
            Translator.sdkS3LocationDescriptionToResourceS3Location(null))
                .isEqualTo(null);
    }

    @Test
    public void sdkCustomPluginLocationDescriptionToResourceCustomPluginLocation_success() {
        assertThat(
            Translator.sdkCustomPluginLocationDescriptionToResourceCustomPluginLocation(
                TestData.FULL_DESCRIBE_CUSTOM_PLUGIN_RESPONSE.latestRevision().location()))
                    .isEqualTo(TestData.FULL_RESOURCE_DESCRIBE_MODEL.getLocation());
        assertThat(
            Translator.sdkCustomPluginLocationDescriptionToResourceCustomPluginLocation(null))
                .isEqualTo(null);
    }

    @Test
    public void resourceS3LocationToSdkS3Location_success() {
        assertThat(
            Translator.resourceS3LocationToSdkS3Location(
                TestData.CREATE_REQUEST_RESOURCE_MODEL.getLocation().getS3Location()))
                    .isEqualTo(TestData.CREATE_CUSTOM_PLUGIN_REQUEST.location().s3Location());
        assertThat(
            Translator.resourceS3LocationToSdkS3Location(null))
                .isEqualTo(null);
    }

    @Test
    public void resourceCustomPluginLocationToSdkCustomPluginLocation_success() {
        assertThat(
            Translator.resourceCustomPluginLocationToSdkCustomPluginLocation(
                TestData.CREATE_REQUEST_RESOURCE_MODEL.getLocation()))
                    .isEqualTo(TestData.CREATE_CUSTOM_PLUGIN_REQUEST.location());
        assertThat(
            Translator.resourceCustomPluginLocationToSdkCustomPluginLocation(null))
                .isEqualTo(null);
    }

    private static class TestData {
        private static final String CUSTOM_PLUGIN_ARN =
            "arn:aws:kafkaconnect:us-east-1:123456789:custom-plugin/unit-test-custom-plugin";
        private static final String CUSTOM_PLUGIN_NAME = "unit-test-custom-plugin";
        private static final String CUSTOM_PLUGIN_ARN_2 =
            "arn:aws:kafkaconnect:us-east-1:123456789:custom-plugin/unit-test-custom-plugin-2";
        private static final String CUSTOM_PLUGIN_NAME_2 = "unit-test-custom-plugin-2";
        private static final String CUSTOM_PLUGIN_DESCRIPTION =
            "Unit testing custom plugin description";
        private static final long CUSTOM_PLUGIN_REVISION = 1L;
        private static final Instant CUSTOM_PLUGIN_CREATION_TIME =
            OffsetDateTime.parse("2021-03-04T14:03:40.818Z").toInstant();
        private static final String CUSTOM_PLUGIN_STATE_CODE = "custom-plugin-state-code";
        private static final String CUSTOM_PLUGIN_STATE_DESCRIPTION = "custom-plugin-state-description";
        private static final String CUSTOM_PLUGIN_FILE_MD5 = "abcd1234";
        private static final long CUSTOM_PLUGIN_FILE_SIZE = 123456L;
        private static final String CUSTOM_PLUGIN_S3_BUCKET_ARN = "bucket-arn";
        private static final String CUSTOM_PLUGIN_S3_FILE_KEY = "file-key";
        private static final String CUSTOM_PLUGIN_S3_OBJECT_VERSION = "object-version";
        private static final String NEXT_TOKEN = "next-token";
        private static final String TAG_KEY_TO_REMOVE = "tag-key-to-remove";

        private static final ResourceModel READ_REQUEST_RESOURCE_MODEL =
            ResourceModel.builder().customPluginArn(CUSTOM_PLUGIN_ARN).build();

        private static final DescribeCustomPluginRequest DESCRIBE_CUSTOM_PLUGIN_REQUEST =
            DescribeCustomPluginRequest.builder().customPluginArn(CUSTOM_PLUGIN_ARN).build();

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

        private static final ResourceModel FULL_RESOURCE_DESCRIBE_MODEL =
            ResourceModel.builder()
                .customPluginArn(CUSTOM_PLUGIN_ARN)
                .description(CUSTOM_PLUGIN_DESCRIPTION)
                .name(CUSTOM_PLUGIN_NAME)
                .fileDescription(
                    CustomPluginFileDescription.builder()
                        .fileMd5(CUSTOM_PLUGIN_FILE_MD5)
                        .fileSize(CUSTOM_PLUGIN_FILE_SIZE)
                        .build())
                .location(
                    CustomPluginLocation.builder()
                        .s3Location(
                            S3Location.builder()
                                .bucketArn(CUSTOM_PLUGIN_S3_BUCKET_ARN)
                                .fileKey(CUSTOM_PLUGIN_S3_FILE_KEY)
                                .objectVersion(CUSTOM_PLUGIN_S3_OBJECT_VERSION)
                                .build())
                        .build())
                .contentType(CustomPluginContentType.ZIP.toString())
                .revision(CUSTOM_PLUGIN_REVISION)
                .build();

        private static final ResourceModel buildBaseModel(final String customPluginArn) {
            return ResourceModel.builder().customPluginArn(customPluginArn).build();
        }

        private static final CustomPluginSummary buildCustomPluginSummary(
            final String customPluginName, final String customPluginArn) {
            return CustomPluginSummary.builder()
                .creationTime(CUSTOM_PLUGIN_CREATION_TIME)
                .customPluginArn(customPluginArn)
                .customPluginState(CustomPluginState.ACTIVE)
                .description(CUSTOM_PLUGIN_DESCRIPTION)
                .name(customPluginName)
                .latestRevision(
                    CustomPluginRevisionSummary.builder()
                        .contentType(CustomPluginContentType.ZIP)
                        .creationTime(CUSTOM_PLUGIN_CREATION_TIME)
                        .description(CUSTOM_PLUGIN_DESCRIPTION)
                        .location(
                            CustomPluginLocationDescription.builder()
                                .s3Location(
                                    S3LocationDescription.builder()
                                        .bucketArn(CUSTOM_PLUGIN_S3_BUCKET_ARN)
                                        .fileKey(CUSTOM_PLUGIN_S3_FILE_KEY)
                                        .objectVersion(CUSTOM_PLUGIN_S3_OBJECT_VERSION)
                                        .build())
                                .build())
                        .fileDescription(
                            software.amazon.awssdk.services.kafkaconnect.model.CustomPluginFileDescription
                                .builder()
                                .fileMd5(CUSTOM_PLUGIN_FILE_MD5)
                                .fileSize(CUSTOM_PLUGIN_FILE_SIZE)
                                .build())
                        .revision(CUSTOM_PLUGIN_REVISION)
                        .build())
                .build();
        }

        private static final List<ResourceModel> LIST_CUSTOM_PLUGIN_MODELS =
            asList(buildBaseModel(CUSTOM_PLUGIN_ARN), buildBaseModel(CUSTOM_PLUGIN_ARN_2));

        private static final ListCustomPluginsRequest LIST_CUSTOM_PLUGINS_REQUEST =
            ListCustomPluginsRequest.builder().nextToken(NEXT_TOKEN).build();

        private static final ListCustomPluginsResponse LIST_CUSTOM_PLUGINS_RESPONSE =
            ListCustomPluginsResponse.builder()
                .customPlugins(
                    asList(
                        buildCustomPluginSummary(CUSTOM_PLUGIN_NAME, CUSTOM_PLUGIN_ARN),
                        buildCustomPluginSummary(CUSTOM_PLUGIN_NAME_2, CUSTOM_PLUGIN_ARN_2)))
                .build();

        private static final UntagResourceRequest UNTAG_RESOURCE_REQUEST =
            UntagResourceRequest.builder()
                .resourceArn(CUSTOM_PLUGIN_ARN)
                .tagKeys(TAG_KEY_TO_REMOVE)
                .build();

        private static final TagResourceRequest TAG_RESOURCE_REQUEST =
            TagResourceRequest.builder().resourceArn(CUSTOM_PLUGIN_ARN).tags(TAGS).build();

        private static final ResourceModel DELETE_REQUEST_RESOURCE_MODEL =
            ResourceModel.builder().customPluginArn(CUSTOM_PLUGIN_ARN).build();

        private static final DeleteCustomPluginRequest DELETE_CUSTOM_PLUGIN_REQUEST =
            DeleteCustomPluginRequest.builder().customPluginArn(CUSTOM_PLUGIN_ARN).build();

        private static final ResourceModel CREATE_REQUEST_RESOURCE_MODEL =
            ResourceModel.builder()
                .name(CUSTOM_PLUGIN_NAME)
                .description(CUSTOM_PLUGIN_DESCRIPTION)
                .contentType(CustomPluginContentType.ZIP.toString())
                .location(
                    CustomPluginLocation.builder()
                        .s3Location(
                            S3Location.builder()
                                .bucketArn(CUSTOM_PLUGIN_S3_BUCKET_ARN)
                                .fileKey(CUSTOM_PLUGIN_S3_FILE_KEY)
                                .objectVersion(CUSTOM_PLUGIN_S3_OBJECT_VERSION)
                                .build())
                        .build())
                .tags(TagHelper.convertToList(TAGS))
                .build();

        private static final CreateCustomPluginRequest CREATE_CUSTOM_PLUGIN_REQUEST =
            CreateCustomPluginRequest.builder()
                .contentType(CustomPluginContentType.ZIP)
                .description(CUSTOM_PLUGIN_DESCRIPTION)
                .location(
                    software.amazon.awssdk.services.kafkaconnect.model.CustomPluginLocation.builder()
                        .s3Location(
                            software.amazon.awssdk.services.kafkaconnect.model.S3Location.builder()
                                .bucketArn(CUSTOM_PLUGIN_S3_BUCKET_ARN)
                                .fileKey(CUSTOM_PLUGIN_S3_FILE_KEY)
                                .objectVersion(CUSTOM_PLUGIN_S3_OBJECT_VERSION)
                                .build())
                        .build())
                .name(CUSTOM_PLUGIN_NAME)
                .tags(TAGS)
                .build();
    }
}
