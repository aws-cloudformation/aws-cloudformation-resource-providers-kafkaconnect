package software.amazon.kafkaconnect.workerconfiguration;

import software.amazon.awssdk.awscore.exception.AwsServiceException;

import software.amazon.awssdk.services.kafkaconnect.model.CreateWorkerConfigurationRequest;
import software.amazon.awssdk.services.kafkaconnect.model.CreateWorkerConfigurationResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.awssdk.services.kafkaconnect.KafkaConnectClient;

public class CreateHandler extends BaseHandlerStd {
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
            .then(progress -> initiateCreateWorkerConfiguration(proxy, proxyClient, progress,
                "AWS-KafkaConnect-WorkerConfiguration::Create", request))
            .then(progress -> readHandler.handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    private ProgressEvent<ResourceModel, CallbackContext> initiateCreateWorkerConfiguration(
        final AmazonWebServicesClientProxy proxy,
        final ProxyClient<KafkaConnectClient> proxyClient,
        final ProgressEvent<ResourceModel, CallbackContext> progress,
        final String callGraph,
        final ResourceHandlerRequest<ResourceModel> request) {

        return proxy
            .initiate(callGraph, proxyClient, progress.getResourceModel(), progress.getCallbackContext())
            .translateToServiceRequest(_resourceModel -> translator.translateToCreateRequest(_resourceModel,
                TagHelper.generateTagsForCreate(request)))
            .makeServiceCall(this::runCreateWorkerConfiguration)
            .done(this::setWorkerConfigurationArn);
    }

    private ProgressEvent<ResourceModel, CallbackContext> setWorkerConfigurationArn(
        final CreateWorkerConfigurationRequest createWorkerConfigurationRequest,
        final CreateWorkerConfigurationResponse createWorkerConfigurationResponse,
        final ProxyClient<KafkaConnectClient> kafkaConnectClientProxyClient,
        final ResourceModel resourceModel,
        final CallbackContext callbackContext) {

        resourceModel.setWorkerConfigurationArn(createWorkerConfigurationResponse.workerConfigurationArn());
        return ProgressEvent.progress(resourceModel, callbackContext);
    }

    private CreateWorkerConfigurationResponse runCreateWorkerConfiguration(
        final CreateWorkerConfigurationRequest createWorkerConfigurationRequest,
        final ProxyClient<KafkaConnectClient> proxyClient) {

        final KafkaConnectClient kafkaConnectClient = proxyClient.client();
        final String identifier = createWorkerConfigurationRequest.name();
        CreateWorkerConfigurationResponse createWorkerConfigurationResponse;

        try {
            createWorkerConfigurationResponse = proxyClient.injectCredentialsAndInvokeV2(
                createWorkerConfigurationRequest,
                kafkaConnectClient::createWorkerConfiguration);
        } catch (final AwsServiceException e) {
            throw exceptionTranslator.translateToCfnException(e, identifier);
        }

        logger.log(String.format("%s [%s] created successfully.", ResourceModel.TYPE_NAME, identifier));
        return createWorkerConfigurationResponse;
    }
}
