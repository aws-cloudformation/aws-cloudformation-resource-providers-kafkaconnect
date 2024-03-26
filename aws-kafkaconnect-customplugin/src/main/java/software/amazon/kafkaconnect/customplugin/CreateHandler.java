package software.amazon.kafkaconnect.customplugin;

import java.time.Duration;
import java.util.function.BiFunction;
import java.util.function.Function;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.kafkaconnect.KafkaConnectClient;
import software.amazon.awssdk.services.kafkaconnect.model.CreateCustomPluginRequest;
import software.amazon.awssdk.services.kafkaconnect.model.CreateCustomPluginResponse;
import software.amazon.awssdk.services.kafkaconnect.model.CustomPluginState;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeCustomPluginRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeCustomPluginResponse;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnResourceConflictException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;

public class CreateHandler extends BaseHandlerStd {
    private static final Constant BACK_OFF_DELAY =
        Constant.of().timeout(Duration.ofHours(1L)).delay(Duration.ofSeconds(30L)).build();
    private static final BiFunction<ResourceModel, ProxyClient<KafkaConnectClient>, ResourceModel> EMPTY_CALL =
        (model, proxyClient) -> model;
    private static final String CUSTOM_PLUGIN_STATE_FAILURE_MESSAGE_PATTERN =
        "%s create request accepted but failed to get state due to: %s";
    private static final String CUSTOM_PLUGIN_STATE_SUCCESS_MESSAGE_PATTERN =
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

        final ResourceModel model = request.getDesiredResourceState();

        return ProgressEvent.progress(model, callbackContext)
            .then(
                progress -> initiateCreateCustomPlugin(
                    proxy, proxyClient, progress, "AWS-KafkaConnect-CustomPlugin::Create", request))
            .then(
                progress -> stabilize(
                    proxy,
                    proxyClient,
                    progress,
                    "AWS-KafkaConnect-CustomPlugin::PostCreateStabilize"))
            .then(
                progress -> readHandler.handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    private ProgressEvent<ResourceModel, CallbackContext> initiateCreateCustomPlugin(
        final AmazonWebServicesClientProxy proxy,
        final ProxyClient<KafkaConnectClient> proxyClient,
        final ProgressEvent<ResourceModel, CallbackContext> progress,
        final String callGraph,
        final ResourceHandlerRequest<ResourceModel> request) {
        return proxy
            .initiate(
                callGraph, proxyClient, progress.getResourceModel(), progress.getCallbackContext())
            .translateToServiceRequest(_resourceModel -> translator.translateToCreateRequest(_resourceModel,
                TagHelper.generateTagsForCreate(request)))
            .makeServiceCall(this::runCreateCustomPlugin)
            .done(this::setCustomPluginArn);
    }

    private ProgressEvent<ResourceModel, CallbackContext> setCustomPluginArn(
        final CreateCustomPluginRequest createCustomPluginRequest,
        final CreateCustomPluginResponse createCustomPluginResponse,
        final ProxyClient<KafkaConnectClient> kafkaConnectClientProxyClient,
        final ResourceModel resourceModel,
        final CallbackContext callbackContext) {
        resourceModel.setCustomPluginArn(createCustomPluginResponse.customPluginArn());
        return ProgressEvent.progress(resourceModel, callbackContext);
    }

    private CreateCustomPluginResponse runCreateCustomPlugin(
        final CreateCustomPluginRequest createCustomPluginRequest,
        final ProxyClient<KafkaConnectClient> proxyClient) {
        final KafkaConnectClient kafkaConnectClient = proxyClient.client();
        final String identifier = createCustomPluginRequest.name();
        CreateCustomPluginResponse createCustomPluginResponse;
        try {
            createCustomPluginResponse =
                proxyClient.injectCredentialsAndInvokeV2(
                    createCustomPluginRequest, kafkaConnectClient::createCustomPlugin);
        } catch (final AwsServiceException e) {
            throw exceptionTranslator.translateToCfnException(e, identifier);
        }

        logger.log(String.format("%s [%s] created successfully.", ResourceModel.TYPE_NAME, identifier));
        return createCustomPluginResponse;
    }

    private ProgressEvent<ResourceModel, CallbackContext> stabilize(
        final AmazonWebServicesClientProxy proxy,
        final ProxyClient<KafkaConnectClient> proxyClient,
        final ProgressEvent<ResourceModel, CallbackContext> progress,
        final String callGraph) {

        return proxy
            .initiate(
                callGraph, proxyClient, progress.getResourceModel(), progress.getCallbackContext())
            .translateToServiceRequest(Function.identity())
            .backoffDelay(BACK_OFF_DELAY)
            .makeServiceCall(EMPTY_CALL)
            .stabilize(
                (request, response, client, model, callbackContext) -> isStabilized(proxyClient, response))
            .progress();
    }

    private boolean isStabilized(
        final ProxyClient<KafkaConnectClient> proxyClient, final ResourceModel model) {
        final KafkaConnectClient kafkaConnectClient = proxyClient.client();
        final CustomPluginState customPluginState =
            getCustomPluginState(
                kafkaConnectClient,
                translator.translateToReadRequest(model),
                proxyClient,
                logger,
                CUSTOM_PLUGIN_STATE_FAILURE_MESSAGE_PATTERN,
                CUSTOM_PLUGIN_STATE_SUCCESS_MESSAGE_PATTERN);
        switch (customPluginState) {
            case ACTIVE:
                return true;
            case CREATING:
                return false;
            case CREATE_FAILED:
                throw new CfnGeneralServiceException(
                    String.format("Couldn't create %s due to create failure", ResourceModel.TYPE_NAME));
            case DELETING:
                throw new CfnResourceConflictException(
                    ResourceModel.TYPE_NAME,
                    model.getCustomPluginArn(),
                    String.format("Another process is deleting this %s", ResourceModel.TYPE_NAME));
            default:
                throw new CfnGeneralServiceException(
                    String.format(
                        "%s create request accepted but current state is unknown",
                        ResourceModel.TYPE_NAME));
        }
    }

    private CustomPluginState getCustomPluginState(
        final KafkaConnectClient kafkaConnectClient,
        final DescribeCustomPluginRequest describeCustomPluginRequest,
        final ProxyClient<KafkaConnectClient> proxyClient,
        final Logger logger,
        final String failureMessagePattern,
        final String successMessagePattern) {
        try {
            final DescribeCustomPluginResponse describeCustomPluginResponse =
                proxyClient.injectCredentialsAndInvokeV2(
                    describeCustomPluginRequest, kafkaConnectClient::describeCustomPlugin);
            final CustomPluginState customPluginState = describeCustomPluginResponse.customPluginState();
            logger.log(
                String.format(
                    successMessagePattern,
                    ResourceModel.TYPE_NAME,
                    describeCustomPluginRequest.customPluginArn(),
                    customPluginState == null ? "unknown" : customPluginState.toString()));

            return customPluginState;
        } catch (final AwsServiceException e) {
            throw new CfnGeneralServiceException(
                String.format(failureMessagePattern, ResourceModel.TYPE_NAME, e.getMessage()), e);
        }
    }
}
