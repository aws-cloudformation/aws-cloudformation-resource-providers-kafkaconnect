package software.amazon.kafkaconnect.connector;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.kafkaconnect.KafkaConnectClient;
import software.amazon.awssdk.services.kafkaconnect.model.ConnectorState;
import software.amazon.awssdk.services.kafkaconnect.model.DeleteConnectorRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DeleteConnectorResponse;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeConnectorRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeConnectorResponse;
import software.amazon.awssdk.services.kafkaconnect.model.NotFoundException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;

import java.time.Duration;
import java.util.function.BiFunction;
import java.util.function.Function;

public class DeleteHandler extends BaseHandlerStd {
    private static final Constant BACK_OFF_DELAY =
        Constant
            .of()
            .timeout(Duration.ofHours(1L))
            .delay(Duration.ofSeconds(30L))
            .build();
    private static final BiFunction<ResourceModel,
        ProxyClient<KafkaConnectClient>,
        ResourceModel> EMPTY_CALL = (model, proxyClient) -> model;
    private Logger logger;
    private final ExceptionTranslator exceptionTranslator;
    private final Translator translator;
    private static final String CONNECTOR_STATE_FAILURE_MESSAGE_PATTERN =
        "Could not initiate deletion of %s. Failed to get state due to: %s";

    public DeleteHandler() {
        this(new ExceptionTranslator(), new Translator());
    }

    /**
     * Constructor used for unit testing
     *
     * @param exceptionTranslator
     * @param translator
     */
    DeleteHandler(final ExceptionTranslator exceptionTranslator, final Translator translator) {
        this.exceptionTranslator = exceptionTranslator;
        this.translator = translator;
    }

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<KafkaConnectClient> proxyClient,
        final Logger logger) {

        this.logger = logger;

        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
            .then(progress -> checkForDeletableConnectorState(proxy, proxyClient, progress,
                "AWS-KafkaConnect-Connector::PreDeleteStateCheck"))
            .then(progress ->
                initiateDeleteConnector(proxy, proxyClient, progress, "AWS-KafkaConnect-Connector::Delete"));
    }

    private ProgressEvent<ResourceModel, CallbackContext> checkForDeletableConnectorState(
        final AmazonWebServicesClientProxy proxy,
        final ProxyClient<KafkaConnectClient> proxyClient,
        final ProgressEvent<ResourceModel, CallbackContext> progress,
        final String callGraph) {

        return proxy.initiate(callGraph, proxyClient, progress.getResourceModel(), progress.getCallbackContext())
            .translateToServiceRequest(Function.identity())
            .backoffDelay(BACK_OFF_DELAY)
            .makeServiceCall(EMPTY_CALL)
            .stabilize((request, response, proxyInvocation, model, callbackContext) ->
                isDeletableConnectorState(proxyClient, response))
            .progress();
    }

    private ProgressEvent<ResourceModel, CallbackContext> initiateDeleteConnector(
        final AmazonWebServicesClientProxy proxy,
        final ProxyClient<KafkaConnectClient> proxyClient,
        final ProgressEvent<ResourceModel, CallbackContext> progress,
        final String callGraph) {

        return proxy.initiate(callGraph, proxyClient, progress.getResourceModel(), progress.getCallbackContext())
            .translateToServiceRequest(translator::translateToDeleteRequest)
            .makeServiceCall(this::runDeleteConnector)
            .stabilize(this::isDeleteStabilized)
            .done(
                awsResponse ->
                    ProgressEvent.<ResourceModel, CallbackContext>builder()
                        .status(OperationStatus.SUCCESS)
                        .build());
    }

    private DeleteConnectorResponse runDeleteConnector(
        final DeleteConnectorRequest deleteConnectorRequest,
        final ProxyClient<KafkaConnectClient> proxyClient) {

        DeleteConnectorResponse deleteConnectorResponse;

        final KafkaConnectClient kafkaConnectClient = proxyClient.client();

        final String identifier = deleteConnectorRequest.connectorArn();

        try {
            deleteConnectorResponse = proxyClient.injectCredentialsAndInvokeV2(
                deleteConnectorRequest,
                kafkaConnectClient::deleteConnector
            );
        } catch (final AwsServiceException e) {
            throw exceptionTranslator.translateToCfnException(e, identifier);
        }

        logger.log(
            String.format(
                "Initiated delete request for %s [%s].",
                ResourceModel.TYPE_NAME,
                identifier
            )
        );

        return deleteConnectorResponse;
    }

    private Boolean isDeletableConnectorState(
        final ProxyClient<KafkaConnectClient> proxyClient,
        final ResourceModel resourceModel) {

        final DescribeConnectorRequest describeConnectorRequest = translator.translateToReadRequest(resourceModel);

        final DescribeConnectorResponse describeConnectorResponse = runDescribeConnectorWithNotFoundCatch(
            describeConnectorRequest, proxyClient, CONNECTOR_STATE_FAILURE_MESSAGE_PATTERN, exceptionTranslator);

        if (ConnectorState.CREATING == describeConnectorResponse.connectorState() ||
            ConnectorState.UPDATING == describeConnectorResponse.connectorState()) {

            return false;
        }

        return true;
    }

    private Boolean isDeleteStabilized(
        final DeleteConnectorRequest deleteConnectorRequest,
        final DeleteConnectorResponse deleteConnectorResponse,
        final ProxyClient<KafkaConnectClient> proxyClient,
        final ResourceModel resourceModel,
        final CallbackContext callbackContext
    ) {
        final String identifier = deleteConnectorResponse.connectorArn();
        final KafkaConnectClient kafkaConnectClient = proxyClient.client();
        final DescribeConnectorRequest describeConnectorRequest =
            translator.translateToReadRequest(resourceModel);

        try {
            final DescribeConnectorResponse response = proxyClient.injectCredentialsAndInvokeV2(
                describeConnectorRequest, kafkaConnectClient::describeConnector);

            // verify delete didn't fail
            if (ConnectorState.FAILED == response.connectorState()) {
                throw new CfnGeneralServiceException(String.format("%s [%s] failed to delete",
                    ResourceModel.TYPE_NAME, identifier));
            }

            return false;
        } catch (final NotFoundException e) {
            logger.log(
                String.format(
                    "%s [%s] successfully deleted.",
                    ResourceModel.TYPE_NAME,
                    identifier
                )
            );
            return true;
        } catch (final AwsServiceException e) {
            throw new CfnGeneralServiceException(
                String.format("%s [%s] deletion status couldn't be retrieved: %s",
                    ResourceModel.TYPE_NAME,
                    identifier,
                    e.getMessage()),
                e);
        }
    }
}
