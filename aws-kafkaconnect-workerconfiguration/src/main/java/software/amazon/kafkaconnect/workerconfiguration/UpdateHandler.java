package software.amazon.kafkaconnect.workerconfiguration;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeWorkerConfigurationRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeWorkerConfigurationResponse;
import software.amazon.awssdk.services.kafkaconnect.model.TagResourceRequest;
import software.amazon.awssdk.services.kafkaconnect.model.UntagResourceRequest;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.exceptions.CfnNotUpdatableException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.awssdk.services.kafkaconnect.KafkaConnectClient;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class UpdateHandler extends BaseHandlerStd {
    private Logger logger;
    private final Translator translator;

    private final ExceptionTranslator exceptionTranslator;

    private final ReadHandler readHandler;

    public UpdateHandler() {
        this(new ExceptionTranslator(), new Translator(), new ReadHandler());
    }

    /**
     * Constructor used for unit testing
     *
     * @param translator
     * @param readHandler
     */
    UpdateHandler(final ExceptionTranslator exceptionTranslator, final Translator translator,
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

        final ResourceModel model = request.getDesiredResourceState();

        return ProgressEvent.progress(model, callbackContext)
            .then(progress -> proxy
                .initiate("AWS-KafkaConnect-WorkerConfiguration::ValidateResourceExists", proxyClient, model,
                    callbackContext)
                .translateToServiceRequest(translator::translateToReadRequest)
                .makeServiceCall(this::validateResourceExists)
                .progress())
            .then(progress -> verifyNonUpdatableFields(model, request.getPreviousResourceState(), progress))
            .then(progress -> updateTags(proxyClient, progress, request))
            .then(progress -> readHandler.handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    private DescribeWorkerConfigurationResponse validateResourceExists(
        DescribeWorkerConfigurationRequest describeWorkerConfigurationRequest,
        ProxyClient<KafkaConnectClient> proxyClient) {
        DescribeWorkerConfigurationResponse describeWorkerConfigurationResponse;
        if (describeWorkerConfigurationRequest.workerConfigurationArn() == null) {
            throw new CfnNotFoundException(ResourceModel.TYPE_NAME, null);
        }
        final String identifier = describeWorkerConfigurationRequest.workerConfigurationArn();

        try {
            describeWorkerConfigurationResponse = proxyClient.injectCredentialsAndInvokeV2(
                describeWorkerConfigurationRequest, proxyClient.client()::describeWorkerConfiguration);
        } catch (final AwsServiceException e) {
            throw exceptionTranslator.translateToCfnException(e, identifier);
        }
        logger.log(String.format("Validated Worker Configuration exists; Name %s",
            describeWorkerConfigurationResponse.name()));
        return describeWorkerConfigurationResponse;
    }

    private ProgressEvent<ResourceModel, CallbackContext> updateTags(final ProxyClient<KafkaConnectClient> proxyClient,
        final ProgressEvent<ResourceModel, CallbackContext> progress, ResourceHandlerRequest<ResourceModel> request) {

        final ResourceModel desiredModel = request.getDesiredResourceState();
        final String identifier = desiredModel.getName();
        final CallbackContext callbackContext = progress.getCallbackContext();

        if (TagHelper.shouldUpdateTags(request)) {
            final Map<String, String> previousTags = TagHelper.getPreviouslyAttachedTags(request);
            final Map<String, String> desiredTags = TagHelper.getNewDesiredTags(request);
            final Map<String, String> addedTags = TagHelper.generateTagsToAdd(previousTags, desiredTags);
            final Set<String> removedTags = TagHelper.generateTagsToRemove(previousTags, desiredTags);

            // calculate tags to remove based on key only
            if (!removedTags.isEmpty()) {
                final UntagResourceRequest untagResourceRequest =
                    Translator.untagResourceRequest(desiredModel, removedTags);
                try {
                    proxyClient.injectCredentialsAndInvokeV2(untagResourceRequest, proxyClient.client()::untagResource);
                    logger.log(String.format("Removed %d tags", removedTags.size()));
                } catch (final AwsServiceException e) {
                    throw exceptionTranslator.translateToCfnException(e, identifier);
                }
            }

            // calculate tags to update based on Tags (key + value)
            if (!addedTags.isEmpty()) {
                final TagResourceRequest tagResourceRequest = Translator.tagResourceRequest(desiredModel, addedTags);
                try {
                    proxyClient.injectCredentialsAndInvokeV2(tagResourceRequest, proxyClient.client()::tagResource);
                    logger.log(String.format("Added %d tags", addedTags.size()));
                } catch (final AwsServiceException e) {
                    throw exceptionTranslator.translateToCfnException(e, identifier);
                }
            }
        }
        return ProgressEvent.progress(desiredModel, callbackContext);

    }

    /**
     * Checks the if the create only fields have been updated and throws an exception if it is the case
     * @param currModel the current resource model
     * @param prevModel the previous resource model
     */
    private ProgressEvent<ResourceModel, CallbackContext> verifyNonUpdatableFields(ResourceModel currModel,
        ResourceModel prevModel,
        ProgressEvent<ResourceModel, CallbackContext> progress) {

        if (prevModel != null) {
            final String identifier = prevModel.getWorkerConfigurationArn();
            if (!Optional.ofNullable(currModel.getName()).equals(Optional.ofNullable(prevModel.getName()))) {
                logger.log("Name change not allowed");
                throw new CfnNotUpdatableException(ResourceModel.TYPE_NAME, identifier);
            }
            if (!Optional.ofNullable(currModel.getDescription())
                .equals(Optional.ofNullable(prevModel.getDescription()))) {
                logger.log(String.format("Description change not allowed; Previous: %s; Current: %s",
                    prevModel.getDescription(), currModel.getDescription()));
                throw new CfnNotUpdatableException(ResourceModel.TYPE_NAME, identifier);
            }
            if (!Optional.ofNullable(currModel.getPropertiesFileContent())
                .equals(Optional.ofNullable(prevModel.getPropertiesFileContent()))) {
                logger.log(String.format("PropertiesFileContent change not allowed; Previous: %s; Current: %s",
                    prevModel.getPropertiesFileContent(), currModel.getPropertiesFileContent()));
                throw new CfnNotUpdatableException(ResourceModel.TYPE_NAME, identifier);

            }
            if (!Optional.ofNullable(currModel.getRevision()).equals(Optional.ofNullable(prevModel.getRevision()))) {
                logger.log(String.format("Revision change not allowed; Previous: %s; Current: %s",
                    prevModel.getRevision(), currModel.getRevision()));
                throw new CfnNotUpdatableException(ResourceModel.TYPE_NAME, identifier);
            }
            if (!Optional.ofNullable(currModel.getWorkerConfigurationArn())
                .equals(Optional.ofNullable(prevModel.getWorkerConfigurationArn()))) {
                logger.log(String.format("WorkerConfigurationArn change not allowed; Previous: %s; Current: %s",
                    prevModel.getWorkerConfigurationArn(), currModel.getWorkerConfigurationArn()));
                throw new CfnNotUpdatableException(ResourceModel.TYPE_NAME, identifier);
            }
        }
        logger.log("Verified non-updatable fields");

        return progress;
    }
}
