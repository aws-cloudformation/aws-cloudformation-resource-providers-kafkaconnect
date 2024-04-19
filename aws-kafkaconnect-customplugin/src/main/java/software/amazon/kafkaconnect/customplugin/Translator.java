package software.amazon.kafkaconnect.customplugin;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import software.amazon.awssdk.services.kafkaconnect.model.CreateCustomPluginRequest;
import software.amazon.awssdk.services.kafkaconnect.model.CustomPluginLocationDescription;
import software.amazon.awssdk.services.kafkaconnect.model.DeleteCustomPluginRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeCustomPluginRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeCustomPluginResponse;
import software.amazon.awssdk.services.kafkaconnect.model.ListCustomPluginsRequest;
import software.amazon.awssdk.services.kafkaconnect.model.ListCustomPluginsResponse;
import software.amazon.awssdk.services.kafkaconnect.model.S3LocationDescription;
import software.amazon.awssdk.services.kafkaconnect.model.TagResourceRequest;
import software.amazon.awssdk.services.kafkaconnect.model.UntagResourceRequest;

/**
 * This class is a centralized placeholder for - api request construction - object translation
 * to/from aws sdk - resource model construction for read/list handlers
 */
public class Translator {

    /**
     * Request to create a resource
     *
     * @param model resource model
     * @return createCustomPluginRequest the kafkaconnect request to create a resource
     */
    public CreateCustomPluginRequest translateToCreateRequest(final ResourceModel model,
        final Map<String, String> tagsForCreate) {
        return CreateCustomPluginRequest.builder()
            .contentType(model.getContentType())
            .description(model.getDescription())
            .location(resourceCustomPluginLocationToSdkCustomPluginLocation(model.getLocation()))
            .name(model.getName())
            .tags(tagsForCreate)
            .build();
    }

    /**
     * Request to read a resource
     *
     * @param model resource model
     * @return describeCustomPluginRequest the kafkaconnect request to describe a resource
     */
    public DescribeCustomPluginRequest translateToReadRequest(final ResourceModel model) {
        return DescribeCustomPluginRequest.builder()
            .customPluginArn(model.getCustomPluginArn())
            .build();
    }

    /**
     * Translates resource object from sdk into a resource model
     *
     * @param describeCustomPluginResponse the kafkaconnect describe resource response
     * @return model resource model
     */
    public ResourceModel translateFromReadResponse(
        final DescribeCustomPluginResponse describeCustomPluginResponse) {
        return ResourceModel.builder()
            .name(describeCustomPluginResponse.name())
            .description(describeCustomPluginResponse.description())
            .customPluginArn(describeCustomPluginResponse.customPluginArn())
            .fileDescription(
                sdkCustomPluginFileDescriptionToResourceCustomPluginFileDescription(
                    describeCustomPluginResponse.latestRevision().fileDescription()))
            .location(
                sdkCustomPluginLocationDescriptionToResourceCustomPluginLocation(
                    describeCustomPluginResponse.latestRevision().location()))
            .contentType(describeCustomPluginResponse.latestRevision().contentTypeAsString())
            .revision(describeCustomPluginResponse.latestRevision().revision())
            .build();
    }

    /**
     * Request to delete a resource.
     *
     * @param model resource model
     * @return awsRequest the aws service request to delete a resource
     */
    public DeleteCustomPluginRequest translateToDeleteRequest(final ResourceModel model) {
        return DeleteCustomPluginRequest.builder().customPluginArn(model.getCustomPluginArn()).build();
    }

    /**
     * Request to list resources
     *
     * @param nextToken token passed to the aws service list resources request
     * @return listCustomPluginsRequest the kafkaconnect request to list resources within aws account
     */
    ListCustomPluginsRequest translateToListRequest(final String nextToken) {
        return ListCustomPluginsRequest.builder().nextToken(nextToken).build();
    }

    /**
     * Translates custom plugins from sdk into a resource model with primary identifier only. This is
     * as per contract for list handlers.
     *
     * <p>Reference -
     * https://docs.aws.amazon.com/cloudformation-cli/latest/userguide/resource-type-test-contract.html#resource-type-test-contract-list
     *
     * @param listCustomPluginsResponse the kafkaconnect list resources response
     * @return list of resource models
     */
    public List<ResourceModel> translateFromListResponse(
        final ListCustomPluginsResponse listCustomPluginsResponse) {
        return streamOfOrEmpty(listCustomPluginsResponse.customPlugins())
            .map(
                customPlugin -> ResourceModel.builder().customPluginArn(customPlugin.customPluginArn()).build())
            .collect(Collectors.toList());
    }

    protected static <T> Stream<T> streamOfOrEmpty(final Collection<T> collection) {
        return Optional.ofNullable(collection).map(Collection::stream).orElseGet(Stream::empty);
    }

    /**
     * Request to add tags to a resource.
     *
     * @param model resource model
     * @return awsRequest the aws service request to create a resource
     */
    static TagResourceRequest translateToTagRequest(
        final ResourceModel model, final Map<String, String> addedTags) {
        return TagResourceRequest.builder()
            .resourceArn(model.getCustomPluginArn())
            .tags(addedTags)
            .build();
    }

    /**
     * Request to remove tags from a resource.
     *
     * @param model resource model
     * @return awsRequest the aws service request to create a resource
     */
    static UntagResourceRequest translateToUntagRequest(
        final ResourceModel model, final Set<String> removedTags) {
        return UntagResourceRequest.builder()
            .resourceArn(model.getCustomPluginArn())
            .tagKeys(removedTags)
            .build();
    }

    protected static CustomPluginFileDescription sdkCustomPluginFileDescriptionToResourceCustomPluginFileDescription(
        final software.amazon.awssdk.services.kafkaconnect.model.CustomPluginFileDescription customPluginFileDescription) {

        return customPluginFileDescription == null
            ? null
            : CustomPluginFileDescription.builder()
                .fileMd5(customPluginFileDescription.fileMd5())
                .fileSize(customPluginFileDescription.fileSize())
                .build();
    }

    protected static CustomPluginLocation sdkCustomPluginLocationDescriptionToResourceCustomPluginLocation(
        final CustomPluginLocationDescription customPluginLocationDescription) {

        return customPluginLocationDescription == null
            ? null
            : CustomPluginLocation.builder()
                .s3Location(
                    sdkS3LocationDescriptionToResourceS3Location(
                        customPluginLocationDescription.s3Location()))
                .build();
    }

    protected static S3Location sdkS3LocationDescriptionToResourceS3Location(
        final S3LocationDescription s3LocationDescription) {

        return s3LocationDescription == null
            ? null
            : S3Location.builder()
                .bucketArn(s3LocationDescription.bucketArn())
                .fileKey(s3LocationDescription.fileKey())
                .objectVersion(s3LocationDescription.objectVersion())
                .build();
    }

    protected static software.amazon.awssdk.services.kafkaconnect.model.CustomPluginLocation resourceCustomPluginLocationToSdkCustomPluginLocation(
        final CustomPluginLocation customPluginLocation) {
        return customPluginLocation == null
            ? null
            : software.amazon.awssdk.services.kafkaconnect.model.CustomPluginLocation.builder()
                .s3Location(resourceS3LocationToSdkS3Location(customPluginLocation.getS3Location()))
                .build();
    }

    protected static software.amazon.awssdk.services.kafkaconnect.model.S3Location resourceS3LocationToSdkS3Location(
        final S3Location s3Location) {
        return s3Location == null
            ? null
            : software.amazon.awssdk.services.kafkaconnect.model.S3Location.builder()
                .bucketArn(s3Location.getBucketArn())
                .fileKey(s3Location.getFileKey())
                .objectVersion(s3Location.getObjectVersion())
                .build();
    }
}
