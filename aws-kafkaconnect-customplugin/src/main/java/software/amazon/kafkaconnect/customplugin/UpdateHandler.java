package software.amazon.kafkaconnect.customplugin;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.kafkaconnect.KafkaConnectClient;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeCustomPluginRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeCustomPluginResponse;
import software.amazon.awssdk.services.kafkaconnect.model.TagResourceRequest;
import software.amazon.awssdk.services.kafkaconnect.model.UntagResourceRequest;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.exceptions.CfnNotUpdatableException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class UpdateHandler extends BaseHandlerStd {

    private Logger logger;

    private final Translator translator;
    private final ExceptionTranslator exceptionTranslator;
    private final ReadHandler readHandler;

    public UpdateHandler() {
        this(new ExceptionTranslator(), new Translator(), new ReadHandler());
    }

    /**
     * This is constructor is used for unit testing.
     *
     * @param exceptionTranslator
     * @param translator
     * @param readHandler
     */
    UpdateHandler(
        final ExceptionTranslator exceptionTranslator,
        final Translator translator,
        final ReadHandler readHandler) {
        this.translator = translator;
        this.exceptionTranslator = exceptionTranslator;
        this.readHandler = readHandler;
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<KafkaConnectClient> proxyClient,
        final Logger logger) {
        this.logger = logger;

        final ResourceModel desiredModel = request.getDesiredResourceState();
        final ResourceModel previousModel = request.getPreviousResourceState();

        return ProgressEvent.progress(desiredModel, callbackContext)
            .then(
                progress -> proxy
                    .initiate(
                        "AWS-KafkaConnect-CustomPlugin::Update::ValidateResourceExists",
                        proxyClient,
                        desiredModel,
                        callbackContext)
                    .translateToServiceRequest(translator::translateToReadRequest)
                    .makeServiceCall(this::validateResourceExists)
                    .progress())
            .then(progress -> verifyNonUpdatableFields(desiredModel, previousModel, progress))
            .then(progress -> updateTags(proxyClient, progress, request))
            .then(
                progress -> readHandler.handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    private DescribeCustomPluginResponse validateResourceExists(
        DescribeCustomPluginRequest describeCustomPluginRequest,
        ProxyClient<KafkaConnectClient> proxyClient) {
        DescribeCustomPluginResponse describeCustomPluginResponse;
        if (describeCustomPluginRequest.customPluginArn() == null) {
            throw new CfnNotFoundException(ResourceModel.TYPE_NAME, null);
        }

        try {
            final KafkaConnectClient kafkaConnectClient = proxyClient.client();
            describeCustomPluginResponse =
                proxyClient.injectCredentialsAndInvokeV2(
                    describeCustomPluginRequest, kafkaConnectClient::describeCustomPlugin);
        } catch (final AwsServiceException e) {
            throw exceptionTranslator.translateToCfnException(
                e, describeCustomPluginRequest.customPluginArn());
        }

        logger.log(
            String.format(
                "Validated Custom Plugin exists with name: %s", describeCustomPluginResponse.name()));
        return describeCustomPluginResponse;
    }

    /**
     * Checks if the CREATE ONLY fields have been updated and throws an exception if it is the case.
     *
     * @param currentModel The current resource model.
     * @param previousModel The previous resource model.
     * @param progress
     * @return
     */
    private ProgressEvent<ResourceModel, CallbackContext> verifyNonUpdatableFields(
        ResourceModel currentModel,
        ResourceModel previousModel,
        ProgressEvent<ResourceModel, CallbackContext> progress) {
        if (previousModel != null) {
            // Check READ ONLY fields.
            final boolean isCustomPluginArnEqual =
                Optional.ofNullable(currentModel.getCustomPluginArn())
                    .equals(Optional.ofNullable(previousModel.getCustomPluginArn()));
            final boolean isRevisionEqual =
                Optional.ofNullable(currentModel.getRevision())
                    .equals(Optional.ofNullable(previousModel.getRevision()));
            final boolean isFileDescriptionEqual =
                Optional.ofNullable(currentModel.getFileDescription())
                    .equals(Optional.ofNullable(previousModel.getFileDescription()));
            // Check CREATE ONLY fields.
            final boolean isNameEqual =
                Optional.ofNullable(currentModel.getName())
                    .equals(Optional.ofNullable(previousModel.getName()));
            final boolean isDescriptionEqual =
                Optional.ofNullable(currentModel.getDescription())
                    .equals(Optional.ofNullable(previousModel.getDescription()));
            final boolean isContentTypeEqual =
                Optional.ofNullable(currentModel.getContentType())
                    .equals(Optional.ofNullable(previousModel.getContentType()));
            final boolean isLocationEqual =
                Optional.ofNullable(currentModel.getLocation())
                    .equals(Optional.ofNullable(previousModel.getLocation()));
            if (!(isCustomPluginArnEqual
                && isRevisionEqual
                && isFileDescriptionEqual
                && isNameEqual
                && isDescriptionEqual
                && isContentTypeEqual
                && isLocationEqual)) {
                throw new CfnNotUpdatableException(
                    ResourceModel.TYPE_NAME, currentModel.getCustomPluginArn());
            }
        }
        logger.log(
            String.format(
                "Verified non-updatable fields for CustomPlugin resource with arn: %s",
                currentModel.getCustomPluginArn()));
        return progress;
    }

    /**
     * Updates the tag for the CustomPlugin. This will remove the tags which are no longer needed and
     * add new tags.
     *
     * @param proxyClient KafkaConnectClient to be used for updating tags
     * @param progress
     * @param request
     * @return
     */
    private ProgressEvent<ResourceModel, CallbackContext> updateTags(
        final ProxyClient<KafkaConnectClient> proxyClient,
        final ProgressEvent<ResourceModel, CallbackContext> progress,
        ResourceHandlerRequest<ResourceModel> request) {
        final ResourceModel desiredModel = request.getDesiredResourceState();
        final String identifier = desiredModel.getCustomPluginArn();

        if (TagHelper.shouldUpdateTags(request)) {
            final Map<String, String> previousTags = TagHelper.getPreviouslyAttachedTags(request);
            final Map<String, String> desiredTags = TagHelper.getNewDesiredTags(request);
            final Map<String, String> addedTags = TagHelper.generateTagsToAdd(previousTags, desiredTags);
            final Set<String> removedTags = TagHelper.generateTagsToRemove(previousTags, desiredTags);
            final KafkaConnectClient kafkaConnectClient = proxyClient.client();

            if (!removedTags.isEmpty()) {
                final UntagResourceRequest untagResourceRequest =
                    Translator.translateToUntagRequest(desiredModel, removedTags);
                try {
                    proxyClient.injectCredentialsAndInvokeV2(
                        untagResourceRequest, kafkaConnectClient::untagResource);
                    logger.log(
                        String.format(
                            "CustomPlugin removed %d tags from arn: %s", removedTags.size(), identifier));
                } catch (final AwsServiceException e) {
                    throw exceptionTranslator.translateToCfnException(e, identifier);
                }
            }

            if (!addedTags.isEmpty()) {
                final TagResourceRequest tagResourceRequest =
                    Translator.translateToTagRequest(desiredModel, addedTags);
                try {
                    proxyClient.injectCredentialsAndInvokeV2(
                        tagResourceRequest, kafkaConnectClient::tagResource);
                    logger.log(
                        String.format("CustomPlugin added %d tags to arn: %s", addedTags.size(), identifier));
                } catch (final AwsServiceException e) {
                    throw exceptionTranslator.translateToCfnException(e, identifier);
                }
            }
        }
        return ProgressEvent.progress(desiredModel, progress.getCallbackContext());
    }
}
