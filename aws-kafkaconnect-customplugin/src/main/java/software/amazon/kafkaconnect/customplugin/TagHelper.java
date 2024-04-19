package software.amazon.kafkaconnect.customplugin;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ObjectUtils;

import software.amazon.awssdk.services.kafkaconnect.KafkaConnectClient;
import software.amazon.awssdk.services.kafkaconnect.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.kafkaconnect.model.ListTagsForResourceResponse;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class TagHelper {
    /**
     * Converts a collection of Tag objects to a tag-name : tag-value map.
     *
     * <p>Note: Tag objects with null tag values will not be included in the output map.
     *
     * @param tags Collection of tags to convert.
     * @return Map of Tag objects.
     */
    public static Map<String, String> convertToMap(final Collection<Tag> tags) {
        if (CollectionUtils.isEmpty(tags)) {
            return Collections.emptyMap();
        }
        return tags.stream()
            .filter(tag -> tag.getValue() != null)
            .collect(Collectors.toMap(Tag::getKey, Tag::getValue, (oldValue, newValue) -> newValue));
    }

    /**
     * Converts a tag map to a list of Tag objects.
     *
     * <p>Note: Like convertToMap, convertToList filters out value-less tag entries.
     *
     * @param tagMap Map of tags to convert.
     * @return List of Tag objects.
     */
    public static List<Tag> convertToList(final Map<String, String> tagMap) {
        if (MapUtils.isEmpty(tagMap)) {
            return Collections.<Tag>emptyList();
        }
        return tagMap.entrySet().stream()
            .filter(tag -> tag.getValue() != null)
            .map(tag -> Tag.builder().key(tag.getKey()).value(tag.getValue()).build())
            .collect(Collectors.toList());
    }

    /**
     * Executes listTagsForResource SDK client call for the specified resource ARN.
     *
     * @param arn Resource ARN to list tags for.
     * @param kafkaConnectClient AWS KafkaConnect client to use.
     * @param proxyClient Proxy client to use for providing credentials.
     * @return ListTagsForResourceResponse from the SDK client call.
     */
    public static ListTagsForResourceResponse listTags(
        final String arn,
        final KafkaConnectClient kafkaConnectClient,
        final ProxyClient<KafkaConnectClient> proxyClient) {
        final ListTagsForResourceRequest listTagsForResourceRequest =
            ListTagsForResourceRequest.builder().resourceArn(arn).build();

        return proxyClient.injectCredentialsAndInvokeV2(
            listTagsForResourceRequest, kafkaConnectClient::listTagsForResource);
    }

    /**
     * generateTagsForCreate
     *
     * Generate tags to put into resource creation request.
     * This includes user defined tags and system tags as well.
     */
    public static Map<String, String> generateTagsForCreate(
        final ResourceHandlerRequest<ResourceModel> handlerRequest) {
        final Map<String, String> tagMap = new HashMap<>();

        // merge system tags with desired resource tags if your service supports CloudFormation system tags
        if (handlerRequest.getSystemTags() != null) {
            tagMap.putAll(handlerRequest.getSystemTags());
        }

        // get desired stack level tags from handlerRequest
        if (handlerRequest.getDesiredResourceTags() != null) {
            tagMap.putAll(handlerRequest.getDesiredResourceTags());
        }

        // get resource level tags from resource model based on your tag property name
        if (handlerRequest.getDesiredResourceState() != null
            && handlerRequest.getDesiredResourceState().getTags() != null) {
            tagMap.putAll(convertToMap(handlerRequest.getDesiredResourceState().getTags()));
        }

        return Collections.unmodifiableMap(tagMap);
    }

    /**
     * shouldUpdateTags
     *
     * Determines whether user defined tags have been changed during update.
     */
    public static boolean shouldUpdateTags(final ResourceHandlerRequest<ResourceModel> handlerRequest) {
        final Map<String, String> previousTags = getPreviouslyAttachedTags(handlerRequest);
        final Map<String, String> desiredTags = getNewDesiredTags(handlerRequest);
        return ObjectUtils.notEqual(previousTags, desiredTags);
    }

    /**
     * getPreviouslyAttachedTags
     *
     * If stack tags and resource tags are not merged together in Configuration class,
     * we will get previous attached user defined tags from both handlerRequest.getPreviousResourceTags (stack tags)
     * and handlerRequest.getPreviousResourceState (resource tags).
     */
    public static Map<String, String> getPreviouslyAttachedTags(
        final ResourceHandlerRequest<ResourceModel> handlerRequest) {
        final Map<String, String> previousTags = new HashMap<>();

        // get previous system tags if your service supports CloudFormation system tags
        if (handlerRequest.getPreviousSystemTags() != null) {
            previousTags.putAll(handlerRequest.getPreviousSystemTags());
        }

        // get previous stack level tags from handlerRequest
        if (handlerRequest.getPreviousResourceTags() != null) {
            previousTags.putAll(handlerRequest.getPreviousResourceTags());
        }

        // get resource level tags from previous resource state based on your tag property name
        if (handlerRequest.getPreviousResourceState() != null
            && handlerRequest.getPreviousResourceState().getTags() != null) {
            previousTags.putAll(convertToMap(handlerRequest.getPreviousResourceState().getTags()));
        }

        return previousTags;
    }

    /**
     * getNewDesiredTags
     *
     * If stack tags and resource tags are not merged together in Configuration class,
     * we will get new user defined tags from both resource model and previous stack tags.
     */
    public static Map<String, String> getNewDesiredTags(final ResourceHandlerRequest<ResourceModel> handlerRequest) {
        final Map<String, String> desiredTags = new HashMap<>();

        // merge system tags with desired resource tags if your service supports CloudFormation system tags
        if (handlerRequest.getSystemTags() != null) {
            desiredTags.putAll(handlerRequest.getSystemTags());
        }

        // get desired stack level tags from handlerRequest
        if (handlerRequest.getDesiredResourceTags() != null) {
            desiredTags.putAll(handlerRequest.getDesiredResourceTags());
        }

        // get resource level tags from resource model based on your tag property name
        desiredTags.putAll(convertToMap(handlerRequest.getDesiredResourceState().getTags()));
        return desiredTags;
    }

    /**
     * Generates a map of tags to be added or modified in the resource.
     *
     * @param previousTags
     * @param desiredTags
     * @return
     */
    public static Map<String, String> generateTagsToAdd(
        final Map<String, String> previousTags, final Map<String, String> desiredTags) {
        return desiredTags.entrySet().stream()
            .filter(
                desiredTag -> !previousTags.containsKey(desiredTag.getKey())
                    || !Objects.equals(
                        previousTags.get(desiredTag.getKey()), desiredTag.getValue()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * getTagsToRemove
     *
     * Determines the tags the customer desired to remove from the function.
     */
    public static Set<String> generateTagsToRemove(final Map<String, String> previousTags,
        final Map<String, String> desiredTags) {
        final Set<String> desiredTagNames = desiredTags.keySet();

        return previousTags.keySet().stream()
            .filter(tagName -> !desiredTagNames.contains(tagName))
            .collect(Collectors.toSet());
    }
}
