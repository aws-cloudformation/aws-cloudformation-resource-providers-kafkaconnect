package software.amazon.kafkaconnect.workerconfiguration;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.kafkaconnect.model.DeleteWorkerConfigurationRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DeleteWorkerConfigurationResponse;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeWorkerConfigurationRequest;
import software.amazon.awssdk.services.kafkaconnect.model.WorkerConfigurationState;
import software.amazon.awssdk.services.kafkaconnect.model.NotFoundException;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeWorkerConfigurationResponse;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.exceptions.CfnNotStabilizedException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.awssdk.services.kafkaconnect.KafkaConnectClient;

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
            .then(progress -> proxy
                .initiate("AWS-KafkaConnect-WorkerConfiguration::ValidateResourceExists", proxyClient, model,
                    callbackContext)
                .translateToServiceRequest(translator::translateToReadRequest)
                .makeServiceCall(this::validateResourceExists)
                .progress())
            .then(progress -> proxy
                .initiate("AWS-KafkaConnect-WorkerConfiguration::Delete", proxyClient, model, callbackContext)
                .translateToServiceRequest(translator::translateToDeleteRequest)
                .makeServiceCall(this::deleteWorkerConfiguration)
                .stabilize(
                    (awsRequest, awsResponse, client, awsModel, context) -> isStabilized(awsRequest, client, awsModel))
                .done(
                    (awsRequest, awsResponse, client, awsModel, context) -> ProgressEvent.defaultSuccessHandler(null)));
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
        } catch (final NotFoundException e) {
            logger.log(String.format("Worker configuration %s does not exist", identifier));
            throw exceptionTranslator.translateToCfnException(e, identifier);
        } catch (final AwsServiceException e) {
            throw exceptionTranslator.translateToCfnException(e, identifier);
        }
        logger.log(String.format("Validated Worker Configuration exists; Name %s",
            describeWorkerConfigurationResponse.name()));
        return describeWorkerConfigurationResponse;
    }

    private DeleteWorkerConfigurationResponse deleteWorkerConfiguration(
        final DeleteWorkerConfigurationRequest deleteWorkerConfigurationRequest,
        final ProxyClient<KafkaConnectClient> proxyClient) {
        DeleteWorkerConfigurationResponse deleteWorkerConfigurationResponse;
        final String identifier = deleteWorkerConfigurationRequest.workerConfigurationArn();

        try {
            deleteWorkerConfigurationResponse =
                proxyClient.injectCredentialsAndInvokeV2(deleteWorkerConfigurationRequest,
                    proxyClient.client()::deleteWorkerConfiguration);
            logger.log(String.format("Deleted Worker Configuration; ARN %s", identifier));

        } catch (final AwsServiceException e) {
            throw exceptionTranslator.translateToCfnException(e, identifier);
        }
        return deleteWorkerConfigurationResponse;
    }

    /**
     * If deletion of your resource requires some form of stabilization (e.g. propagation delay)
     * for more information -> https://docs.aws.amazon.com/cloudformation-cli/latest/userguide/resource-type-test-contract.html
     * @param deleteWorkerConfigurationRequest the aws service request to delete a resource
     * @param proxyClient the aws service client to make the call
     * @param model resource model
     * @return boolean state of stabilized or not
     */
    private boolean isStabilized(
        final DeleteWorkerConfigurationRequest deleteWorkerConfigurationRequest,
        final ProxyClient<KafkaConnectClient> proxyClient,
        final ResourceModel model) {

        final String identifier = deleteWorkerConfigurationRequest.workerConfigurationArn();

        try {
            final WorkerConfigurationState currentWorkerConfigurationState =
                proxyClient.injectCredentialsAndInvokeV2(translator.translateToReadRequest(model),
                    proxyClient.client()::describeWorkerConfiguration).workerConfigurationState();

            switch (currentWorkerConfigurationState) {
                case DELETING:
                    logger.log(String.format("Worker configuration %s is deleting, current state is %s", identifier,
                        currentWorkerConfigurationState));
                    return false;
                default:
                    logger.log(String.format("Worker configuration %s reached unexpected state %s", identifier,
                        currentWorkerConfigurationState));
                    throw new CfnNotStabilizedException(
                        ResourceModel.TYPE_NAME, identifier);
            }
        } catch (final NotFoundException e) {
            logger.log(String.format("Worker configuration %s is deleted", identifier));
            return true;
        } catch (final AwsServiceException e) {
            throw exceptionTranslator.translateToCfnException(e, identifier);
        }
    }
}
