package software.amazon.kafkaconnect.customplugin;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.kafkaconnect.KafkaConnectClient;
import software.amazon.awssdk.services.kafkaconnect.model.CustomPluginState;
import software.amazon.awssdk.services.kafkaconnect.model.DeleteCustomPluginRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DeleteCustomPluginResponse;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeCustomPluginRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeCustomPluginResponse;
import software.amazon.awssdk.services.kafkaconnect.model.NotFoundException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.exceptions.CfnNotStabilizedException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class DeleteHandler extends BaseHandlerStd {
    private Logger logger;

    private final Translator translator;
    private final ExceptionTranslator exceptionTranslator;

    public DeleteHandler() {
        this(new ExceptionTranslator(), new Translator());
    }

    /**
     * Constructor used for unit testing
     *
     * @param translator
     */
    DeleteHandler(final ExceptionTranslator exceptionTranslator, final Translator translator) {
        this.translator = translator;
        this.exceptionTranslator = exceptionTranslator;
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
            .then(
                progress -> proxy
                    .initiate(
                        "AWS-KafkaConnect-CustomPlugin::ValidateResourceExists",
                        proxyClient,
                        model,
                        callbackContext)
                    .translateToServiceRequest(translator::translateToReadRequest)
                    .makeServiceCall(this::validateResourceExists).progress())
            .then(
                progress -> proxy
                    .initiate(
                        "AWS-KafkaConnect-CustomPlugin::Delete",
                        proxyClient,
                        model,
                        callbackContext)
                    .translateToServiceRequest(translator::translateToDeleteRequest)
                    .makeServiceCall(this::deleteCustomPlugin)
                    .stabilize(
                        (awsRequest, awsResponse, client, awsModel, context) -> isStabilized(awsRequest, client,
                            awsModel))
                    .done(
                        (awsRequest, awsResponse, client, awsModel, context) -> ProgressEvent
                            .defaultSuccessHandler(null)));
    }

    private DescribeCustomPluginResponse validateResourceExists(
        DescribeCustomPluginRequest describeCustomPluginRequest,
        ProxyClient<KafkaConnectClient> proxyClient) {
        DescribeCustomPluginResponse describeCustomPluginResponse;
        if (describeCustomPluginRequest.customPluginArn() == null) {
            throw new CfnNotFoundException(ResourceModel.TYPE_NAME, null);
        }
        final String identifier = describeCustomPluginRequest.customPluginArn();
        try {
            final KafkaConnectClient kafkaConnectClient = proxyClient.client();
            describeCustomPluginResponse =
                proxyClient.injectCredentialsAndInvokeV2(
                    describeCustomPluginRequest, kafkaConnectClient::describeCustomPlugin);
        } catch (final NotFoundException e) {
            logger.log(
                String.format("%s with arn: %s does not exist!", ResourceModel.TYPE_NAME, identifier));
            throw exceptionTranslator.translateToCfnException(e, identifier);
        } catch (final AwsServiceException e) {
            throw exceptionTranslator.translateToCfnException(e, identifier);
        }
        logger.log(
            String.format(
                "Validated %s with arn: %s name: %s exists!",
                ResourceModel.TYPE_NAME, identifier, describeCustomPluginResponse.name()));
        return describeCustomPluginResponse;
    }

    private DeleteCustomPluginResponse deleteCustomPlugin(
        final DeleteCustomPluginRequest deleteCustomPluginRequest,
        final ProxyClient<KafkaConnectClient> proxyClient) {
        DeleteCustomPluginResponse deleteCustomPluginResponse;
        final String identifier = deleteCustomPluginRequest.customPluginArn();
        try {
            final KafkaConnectClient kafkaConnectClient = proxyClient.client();
            deleteCustomPluginResponse =
                proxyClient.injectCredentialsAndInvokeV2(
                    deleteCustomPluginRequest, kafkaConnectClient::deleteCustomPlugin);
            logger.log(
                String.format(
                    "Initiated delete procedure for %s with arn: %s",
                    ResourceModel.TYPE_NAME, identifier));
        } catch (final AwsServiceException e) {
            throw exceptionTranslator.translateToCfnException(e, identifier);
        }
        return deleteCustomPluginResponse;
    }

    private boolean isStabilized(
        final DeleteCustomPluginRequest deleteCustomPluginRequest,
        final ProxyClient<KafkaConnectClient> proxyClient,
        final ResourceModel model) {
        final String identifier = deleteCustomPluginRequest.customPluginArn();
        try {
            final KafkaConnectClient kafkaConnectClient = proxyClient.client();
            final CustomPluginState customPluginState =
                proxyClient
                    .injectCredentialsAndInvokeV2(
                        translator.translateToReadRequest(model),
                        kafkaConnectClient::describeCustomPlugin)
                    .customPluginState();
            switch (customPluginState) {
                case DELETING:
                    logger.log(
                        String.format(
                            "%s with arn: %s is being deleted...", ResourceModel.TYPE_NAME, identifier));
                    return false;
                default:
                    logger.log(
                        String.format(
                            "%s with arn: %s reached unexpected state: %s",
                            ResourceModel.TYPE_NAME, identifier, customPluginState));
                    throw new CfnNotStabilizedException(ResourceModel.TYPE_NAME, identifier);
            }
        } catch (final NotFoundException e) {
            logger.log(String.format("Deleted %s with arn: %s", ResourceModel.TYPE_NAME, identifier));
            return true;
        } catch (final AwsServiceException e) {
            throw exceptionTranslator.translateToCfnException(e, identifier);
        }
    }
}
