package software.amazon.kafkaconnect.connector;

import lombok.NonNull;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.kafkaconnect.KafkaConnectClient;
import software.amazon.awssdk.services.kafkaconnect.model.ConnectorState;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeConnectorRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeConnectorResponse;
import software.amazon.awssdk.services.kafkaconnect.model.UpdateConnectorRequest;
import software.amazon.awssdk.services.kafkaconnect.model.UpdateConnectorResponse;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnNotUpdatableException;
import software.amazon.cloudformation.exceptions.CfnResourceConflictException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;

import java.time.Duration;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

public class UpdateHandler extends BaseHandlerStd {
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
        "%s update request accepted but failed to get state due to: %s";
    private static final String CONNECTOR_STATE_SUCCESS_MESSAGE_PATTERN =
        "Update state of resource %s with ID %s is %s";
    private static final String DESCRIBE_STATE_FAILURE_MESSAGE_PATTERN =
        "Could not update %s due to read failure from %s";
    private static final String DESCRIBE_FAILURE_MESSAGE_PATTERN =
        "%s update request accepted but failed to read due to: %s";

    private Logger logger;
    private final ExceptionTranslator exceptionTranslator;
    private final Translator translator;
    private final ReadHandler readHandler;

    public UpdateHandler() {
        this(new ExceptionTranslator(), new Translator(), new ReadHandler());
    }

    /**
     * Constructor used for unit testing
     *
     * @param exceptionTranslator
     * @param translator
     * @param readHandler
     */
    UpdateHandler(final ExceptionTranslator exceptionTranslator, final Translator translator,
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
                verifyUpdatable(proxy, proxyClient, progress, "AWS-KafkaConnect-Connector::PreUpdateCheck"))
            .then(progress ->
                initiateUpdateConnector(proxy, proxyClient, progress, "AWS-KafkaConnect-Connector::Update"))
            .then(progress ->
                stabilize(proxy, proxyClient, progress, "AWS-KafkaConnect-Connector::PostUpdateStabilize"))
            .then(progress -> readHandler.handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    private ProgressEvent<ResourceModel, CallbackContext> verifyUpdatable(
        final AmazonWebServicesClientProxy proxy,
        final ProxyClient<KafkaConnectClient> proxyClient,
        final ProgressEvent<ResourceModel, CallbackContext> progress,
        final String callGraph) {

        return proxy.initiate(callGraph, proxyClient, progress.getResourceModel(), progress.getCallbackContext())
            .translateToServiceRequest(translator::translateToReadRequest)
            .makeServiceCall(this::verifyResourceRunning)
            .done(this::verifyUpdateFieldsNotCreateOnly);
    }

    private ProgressEvent<ResourceModel, CallbackContext> verifyUpdateFieldsNotCreateOnly(
        final DescribeConnectorRequest describeConnectorRequest,
        final DescribeConnectorResponse describeConnectorResponse,
        final ProxyClient<KafkaConnectClient> kafkaConnectClientProxyClient,
        final ResourceModel updateRequest,
        final CallbackContext callbackContext) {

        final ResourceModel describeResult =
            translator.translateFromReadResponse(describeConnectorResponse);
        final String identifier = updateRequest.getConnectorArn();

        // verify updatability by comparing all createOnly fields
        final boolean isNameEqual = Objects.equals(updateRequest.getConnectorName(), describeResult.getConnectorName());
        final boolean isConfigEqual =
            Objects.equals(updateRequest.getConnectorConfiguration(), describeResult.getConnectorConfiguration());
        final boolean isDescriptionEqual =
            Objects.equals(updateRequest.getConnectorDescription(), describeResult.getConnectorDescription());
        final boolean isKafkaClusterEqual = isKafkaClusterEqual(updateRequest.getKafkaCluster(),
            describeResult.getKafkaCluster());
        final boolean isKafkaClusterClientAuthenticationEqual = Objects.equals(
            updateRequest.getKafkaClusterClientAuthentication(), describeResult.getKafkaClusterClientAuthentication());
        final boolean isKafkaConnectVersionEqual =
            Objects.equals(updateRequest.getKafkaConnectVersion(), describeResult.getKafkaConnectVersion());
        final boolean isLogDeliveryEqual =
            Objects.equals(updateRequest.getLogDelivery(), describeResult.getLogDelivery());
        final boolean isPluginsEqual = Objects.equals(updateRequest.getPlugins(), describeResult.getPlugins());
        final boolean isServiceExecutionRoleEqual =
            Objects.equals(updateRequest.getServiceExecutionRoleArn(), describeResult.getServiceExecutionRoleArn());
        final boolean isWorkerConfigurationEqual =
            Objects.equals(updateRequest.getWorkerConfiguration(), describeResult.getWorkerConfiguration());

        final boolean isUpdatable = isNameEqual && isConfigEqual && isDescriptionEqual && isKafkaClusterEqual &&
            isKafkaClusterClientAuthenticationEqual && isKafkaConnectVersionEqual && isLogDeliveryEqual &&
            isPluginsEqual && isServiceExecutionRoleEqual && isWorkerConfigurationEqual;

        if (!isUpdatable) {
            throw new CfnNotUpdatableException(ResourceModel.TYPE_NAME, identifier);
        }

        return ProgressEvent.progress(updateRequest, callbackContext);
    }

    private DescribeConnectorResponse verifyResourceRunning(
        final DescribeConnectorRequest describeConnectorRequest,
        final ProxyClient<KafkaConnectClient> proxyClient) {

        final DescribeConnectorResponse describeConnectorResponse = runDescribeConnectorWithNotFoundCatch(
            describeConnectorRequest, proxyClient, DESCRIBE_STATE_FAILURE_MESSAGE_PATTERN, exceptionTranslator);
        final String identifier = describeConnectorRequest.connectorArn();

        if (ConnectorState.RUNNING != describeConnectorResponse.connectorState()) {
            throw new CfnNotUpdatableException(ResourceModel.TYPE_NAME, identifier);
        }

        logger.log(String.format("State of resource %s with ID %s is RUNNING", ResourceModel.TYPE_NAME, identifier));

        return describeConnectorResponse;
    }

    private ProgressEvent<ResourceModel, CallbackContext> initiateUpdateConnector(
        final AmazonWebServicesClientProxy proxy,
        final ProxyClient<KafkaConnectClient> proxyClient,
        final ProgressEvent<ResourceModel, CallbackContext> progress,
        final String callGraph) {

        return proxy.initiate(callGraph, proxyClient, progress.getResourceModel(), progress.getCallbackContext())
            .translateToServiceRequest(translator::translateToUpdateRequest)
            .makeServiceCall(this::runUpdateConnector)
            .progress();
    }

    private UpdateConnectorResponse runUpdateConnector(
        final UpdateConnectorRequest updateConnectorRequest,
        final ProxyClient<KafkaConnectClient> proxyClient) {

        final KafkaConnectClient kafkaConnectClient = proxyClient.client();
        final String identifier = updateConnectorRequest.connectorArn();
        final UpdateConnectorRequest requestWithCurrentVersion = updateConnectorRequest
            .copy(request -> request.currentVersion(getCurrentVersion(identifier, proxyClient)));

        UpdateConnectorResponse updateConnectorResponse;

        try {
            updateConnectorResponse = proxyClient.injectCredentialsAndInvokeV2(
                requestWithCurrentVersion, kafkaConnectClient::updateConnector);
        } catch (final AwsServiceException e) {
            throw exceptionTranslator.translateToCfnException(e, identifier);
        }

        logger.log(
            String.format(
                "%s [%s] update successfully initiated.",
                ResourceModel.TYPE_NAME,
                identifier
            ));
        return updateConnectorResponse;
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
                (request, response, proxyInvocation, model, callbackContext) ->
                    isStabilized(proxyClient, response, request))
            .progress();
    }

    private Boolean isStabilized(
        final ProxyClient<KafkaConnectClient> proxyClient,
        final ResourceModel responseResourceModel,
        final ResourceModel requestResourceModel) {

        final DescribeConnectorRequest describeConnectorRequest = translator.translateToReadRequest(
            responseResourceModel);
        final DescribeConnectorResponse describeConnectorResponse = runDescribeConnector(describeConnectorRequest,
            proxyClient, CONNECTOR_STATE_FAILURE_MESSAGE_PATTERN);
        final ConnectorState connectorState = describeConnectorResponse.connectorState();

        logger.log(String.format(CONNECTOR_STATE_SUCCESS_MESSAGE_PATTERN, ResourceModel.TYPE_NAME,
            describeConnectorRequest.connectorArn(), connectorState == null ? "unknown" : connectorState.toString()));

        switch (connectorState) {
            case RUNNING:
                throwExceptionIfUpdateNotSuccessful(describeConnectorResponse, requestResourceModel);
                return true;
            case UPDATING:
                return false;
            case FAILED:
                throw new CfnGeneralServiceException(
                    String.format("Couldn't update %s due to update failure", ResourceModel.TYPE_NAME));
            case DELETING:
                throw new CfnResourceConflictException(
                    ResourceModel.TYPE_NAME,
                    responseResourceModel.getConnectorArn(),
                    String.format("Another process is deleting this %s", ResourceModel.TYPE_NAME));
            case CREATING:
                throw new CfnResourceConflictException(
                    ResourceModel.TYPE_NAME,
                    responseResourceModel.getConnectorArn(),
                    String.format("This %s is being created and cannot be updated", ResourceModel.TYPE_NAME));
            default:
                throw new CfnGeneralServiceException(String
                    .format("%s update request accepted but current state is unknown", ResourceModel.TYPE_NAME));
        }
    }

    private String getCurrentVersion(final String connectorArn,
        final ProxyClient<KafkaConnectClient> proxyClient) {

        final DescribeConnectorResponse describeConnectorResponse = runDescribeConnector(
            DescribeConnectorRequest.builder().connectorArn(connectorArn).build(),
            proxyClient,
            DESCRIBE_FAILURE_MESSAGE_PATTERN);
        return describeConnectorResponse.currentVersion();
    }

    private void throwExceptionIfUpdateNotSuccessful(
        final DescribeConnectorResponse describeConnectorResponse,
        final ResourceModel requestResourceModel) {

        final ResourceModel updatedResourceModel = translator.translateFromReadResponse(describeConnectorResponse);

        if (!Objects.equals(requestResourceModel.getCapacity(), updatedResourceModel.getCapacity())) {
            throw new CfnGeneralServiceException(
                String.format("Couldn't update %s due to update failure. Resource reverted to previous state",
                    ResourceModel.TYPE_NAME));
        }
    }

    /**
     * Equality check relies on all parameters within the KafkaCluster being required fields in the schema.
     */
    private boolean isKafkaClusterEqual(@NonNull final KafkaCluster kafkaCluster1,
        @NonNull final KafkaCluster kafkaCluster2) {

        final ApacheKafkaCluster apacheKafkaCluster1 = kafkaCluster1.getApacheKafkaCluster();
        final ApacheKafkaCluster apacheKafkaCluster2 = kafkaCluster2.getApacheKafkaCluster();

        final boolean isBootstrapServersEqual = Objects.equals(apacheKafkaCluster1.getBootstrapServers(),
            apacheKafkaCluster2.getBootstrapServers());
        final boolean isSecurityGroupsEqual = Objects.equals(apacheKafkaCluster1.getVpc().getSecurityGroups(),
            apacheKafkaCluster2.getVpc().getSecurityGroups());
        final boolean isSubnetsEqual = Objects.equals(apacheKafkaCluster2.getVpc().getSubnets(),
            apacheKafkaCluster2.getVpc().getSubnets());

        return isBootstrapServersEqual && isSecurityGroupsEqual && isSubnetsEqual;
    }
}
