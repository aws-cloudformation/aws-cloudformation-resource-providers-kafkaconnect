package software.amazon.kafkaconnect.workerconfiguration;

import software.amazon.awssdk.services.kafkaconnect.model.CreateWorkerConfigurationRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeWorkerConfigurationRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeWorkerConfigurationResponse;
import software.amazon.awssdk.services.kafkaconnect.model.ListWorkerConfigurationsRequest;
import software.amazon.awssdk.services.kafkaconnect.model.ListWorkerConfigurationsResponse;
import software.amazon.awssdk.services.kafkaconnect.model.DeleteWorkerConfigurationRequest;
import software.amazon.awssdk.services.kafkaconnect.model.TagResourceRequest;
import software.amazon.awssdk.services.kafkaconnect.model.UntagResourceRequest;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class is a centralized placeholder for
 * - api request construction
 * - object translation to/from aws sdk
 * - resource model construction for read/list handlers
 */

public class Translator {

    public Translator() {
    }

    /**
     * Request to create a resource
     *
     * @param model resource model
     * @return createWorkerConfigurationRequest the kafkaconnect request to create a resource
     */
    public CreateWorkerConfigurationRequest translateToCreateRequest(final ResourceModel model,
        final Map<String, String> tagsForCreate) {
        return CreateWorkerConfigurationRequest.builder()
            .name(model.getName())
            .description(model.getDescription())
            .propertiesFileContent(model.getPropertiesFileContent())
            .tags(tagsForCreate)
            .build();
    }

    /**
     * Request to read a resource
     *
     * @param model resource model
     * @return describeWorkerConfigurationRequest the kafkaconnect request to describe a resource
     */
    public DescribeWorkerConfigurationRequest translateToReadRequest(final ResourceModel model) {
        return DescribeWorkerConfigurationRequest.builder()
            .workerConfigurationArn(model.getWorkerConfigurationArn())
            .build();
    }

    /**
     * Translates resource object from sdk into a resource model
     *
     * @param describeWorkerConfigurationResponse the kafkaconnect describe resource response
     * @return model resource model
     */
    public ResourceModel translateFromReadResponse(
        final DescribeWorkerConfigurationResponse describeWorkerConfigurationResponse) {
        return ResourceModel
            .builder()
            .name(describeWorkerConfigurationResponse.name())
            .description(describeWorkerConfigurationResponse.description())
            .workerConfigurationArn(describeWorkerConfigurationResponse.workerConfigurationArn())
            .revision(describeWorkerConfigurationResponse.latestRevision().revision())
            .propertiesFileContent(describeWorkerConfigurationResponse.latestRevision().propertiesFileContent())
            .build();
    }

    /**
     * Request to delete a resource
     *
     * @param model resource model
     * @return awsRequest the aws service request to delete a resource
     */
    public DeleteWorkerConfigurationRequest translateToDeleteRequest(final ResourceModel model) {
        return DeleteWorkerConfigurationRequest.builder()
            .workerConfigurationArn(model.getWorkerConfigurationArn())
            .build();
    }

    /**
     * Request to list resources
     *
     * @param nextToken token passed to the aws service list resources request
     * @return awsRequest the aws service request to list resources within aws account
     */
    public ListWorkerConfigurationsRequest translateToListRequest(final String nextToken) {
        return ListWorkerConfigurationsRequest.builder()
            .nextToken(nextToken)
            .build();
    }

    /**
     * Translates resource objects from sdk into a resource model (primary identifier only)
     *
     * @param listWorkerConfigurationsResponse the aws service list resources response
     * @return list of resource models
     */
    public List<ResourceModel> translateFromListResponse(
        final ListWorkerConfigurationsResponse listWorkerConfigurationsResponse) {
        return streamOfOrEmpty(listWorkerConfigurationsResponse.workerConfigurations())
            .map(workerConfiguration -> ResourceModel.builder()
                .workerConfigurationArn(workerConfiguration.workerConfigurationArn())
                .build())
            .collect(Collectors.toList());
    }

    private static <T> Stream<T> streamOfOrEmpty(final Collection<T> collection) {
        return Optional.ofNullable(collection)
            .map(Collection::stream)
            .orElseGet(Stream::empty);
    }

    /**
     * Request to add tags to a resource
     *
     * @param model resource model
     * @return awsRequest the aws service request to create a resource
     */
    static TagResourceRequest tagResourceRequest(final ResourceModel model, final Map<String, String> addedTags) {
        return TagResourceRequest.builder()
            .resourceArn(model.getWorkerConfigurationArn())
            .tags(addedTags)
            .build();
    }

    /**
     * Request to add tags to a resource
     *
     * @param model resource model
     * @return awsRequest the aws service request to create a resource
     */
    static UntagResourceRequest untagResourceRequest(final ResourceModel model, final Set<String> removedTags) {
        return UntagResourceRequest.builder()
            .resourceArn(model.getWorkerConfigurationArn())
            .tagKeys(removedTags)
            .build();

    }
}
