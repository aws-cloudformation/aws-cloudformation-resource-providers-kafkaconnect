package software.amazon.kafkaconnect.connector;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.kafkaconnect.KafkaConnectClient;
import software.amazon.awssdk.services.kafkaconnect.model.ConnectorState;
import software.amazon.awssdk.services.kafkaconnect.model.CreateConnectorRequest;
import software.amazon.awssdk.services.kafkaconnect.model.CreateConnectorResponse;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeConnectorRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeConnectorResponse;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnResourceConflictException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;

import java.time.Duration;
import java.util.function.BiFunction;
import java.util.function.Function;

public class CreateHandler extends BaseHandlerStd {
    private static final Constant BACK_OFF_DELAY =
        Constant
            .of()
            .timeout(Duration.ofHours(1L))
            .delay(Duration.ofSeconds(30L))
            .build();
    private static final BiFunction<ResourceModel,
        ProxyClient<KafkaConnectClient>,
        ResourceModel> EMPTY_CALL = (model, proxyClient) -> model;
    private static final String CONNECTOR_STATE_FAILURE_MESSAGE_PATTERN =
        "%s create request accepted but failed to get state due to: %s";
    private static final String CONNECTOR_STATE_SUCCESS_MESSAGE_PATTERN =
        "Create state of resource %s with ID %s is %s";

    private Logger logger;
    private final ExceptionTranslator exceptionTranslator;
    private final Translator translator;
    private final ReadHandler readHandler;

    public CreateHandler() {
        this(new ExceptionTranslator(), new Translator(), new ReadHandler());
    }

    /**
     * Constructor used for unit testing
     *
     * @param exceptionTranslator
     * @param translator
     * @param readHandler
     */
    CreateHandler(
        final ExceptionTranslator exceptionTranslator,
        final Translator translator,
        final ReadHandler readHandler) {

        this.exceptionTranslator = exceptionTranslator;
        this.translator = translator;
        this.readHandler = readHandler;
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
            .then(progress ->
                initiateCreateConnector(proxy, proxyClient, progress, "AWS-KafkaConnect-Connector::Create"))
            .then(progress ->
                stabilize(proxy, proxyClient, progress, "AWS-KafkaConnect-Connector::PostCreateStabilize"))
            .then(progress -> readHandler.handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    private ProgressEvent<ResourceModel, CallbackContext> initiateCreateConnector(
        final AmazonWebServicesClientProxy proxy,
        final ProxyClient<KafkaConnectClient> proxyClient,
        final ProgressEvent<ResourceModel, CallbackContext> progress,
        final String callGraph) {

        return proxy.initiate(callGraph, proxyClient, progress.getResourceModel(), progress.getCallbackContext())
            .translateToServiceRequest(translator::translateToCreateRequest)
            .makeServiceCall(this::runCreateConnector)
            .done(this::setConnectorArn);
    }

    private ProgressEvent<ResourceModel, CallbackContext> setConnectorArn(
        final CreateConnectorRequest createConnectorRequest,
        final CreateConnectorResponse createConnectorResponse,
        final ProxyClient<KafkaConnectClient> kafkaConnectClientProxyClient,
        final ResourceModel resourceModel,
        final CallbackContext callbackContext) {

        resourceModel.setConnectorArn(createConnectorResponse.connectorArn());
        return ProgressEvent.progress(resourceModel, callbackContext);
    }

    private CreateConnectorResponse runCreateConnector(
        final CreateConnectorRequest createConnectorRequest,
        final ProxyClient<KafkaConnectClient> proxyClient) {

        final KafkaConnectClient kafkaConnectClient = proxyClient.client();
        final String identifier = createConnectorRequest.connectorName();
        CreateConnectorResponse createConnectorResponse;

        try {
            createConnectorResponse = proxyClient.injectCredentialsAndInvokeV2(
                createConnectorRequest,
                kafkaConnectClient::createConnector
            );
        } catch (final AwsServiceException e) {
            throw exceptionTranslator.translateToCfnException(e, identifier);
        }

        logger.log(String.format("%s [%s] create successfully initiated.", ResourceModel.TYPE_NAME, identifier));
        return createConnectorResponse;
    }

    private ProgressEvent<ResourceModel, CallbackContext> stabilize(
        final AmazonWebServicesClientProxy proxy,
        final ProxyClient<KafkaConnectClient> proxyClient,
        final ProgressEvent<ResourceModel, CallbackContext> progress,
        final String callGraph) {

        return proxy.initiate(callGraph, proxyClient, progress.getResourceModel(), progress.getCallbackContext())
            .translateToServiceRequest(Function.identity())
            .backoffDelay(BACK_OFF_DELAY)
            .makeServiceCall(EMPTY_CALL)
            .stabilize(
                (request, response, proxyInvocation, model, callbackContext) -> isStabilized(proxyClient, response))
            .progress();
    }

    private Boolean isStabilized(
        final ProxyClient<KafkaConnectClient> proxyClient,
        final ResourceModel resourceModel) {

        final ConnectorState state = getConnectorState(translator.translateToReadRequest(resourceModel),
            proxyClient, logger, CONNECTOR_STATE_FAILURE_MESSAGE_PATTERN, CONNECTOR_STATE_SUCCESS_MESSAGE_PATTERN);

        switch (state) {
            case RUNNING:
                return true;
            case CREATING:
                return false;
            case FAILED:
                throw new CfnGeneralServiceException(
                    String.format("Couldn't create %s due to create failure", ResourceModel.TYPE_NAME));
            case UPDATING:
                throw new CfnResourceConflictException(
                    ResourceModel.TYPE_NAME,
                    resourceModel.getConnectorArn(),
                    String.format("Another process is updating this %s", ResourceModel.TYPE_NAME));
            case DELETING:
                throw new CfnResourceConflictException(
                    ResourceModel.TYPE_NAME,
                    resourceModel.getConnectorArn(),
                    String.format("Another process is deleting this %s", ResourceModel.TYPE_NAME));
            default:
                throw new CfnGeneralServiceException(String
                    .format("%s create request accepted but current state is unknown", ResourceModel.TYPE_NAME));
        }
    }

    private ConnectorState getConnectorState(
        final DescribeConnectorRequest describeConnectorRequest,
        final ProxyClient<KafkaConnectClient> proxyClient,
        final Logger logger,
        final String failureMessagePattern,
        final String successMessagePattern) {

        final DescribeConnectorResponse describeConnectorResponse =
            runDescribeConnector(describeConnectorRequest, proxyClient, failureMessagePattern);

        final ConnectorState connectorState = describeConnectorResponse.connectorState();

        logger.log(String.format(successMessagePattern, ResourceModel.TYPE_NAME,
            describeConnectorRequest.connectorArn(), connectorState == null ? "unknown" : connectorState.toString()));

        return connectorState;
    }
}
