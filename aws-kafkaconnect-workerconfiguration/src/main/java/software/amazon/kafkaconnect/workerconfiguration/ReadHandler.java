package software.amazon.kafkaconnect.workerconfiguration;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.kafkaconnect.KafkaConnectClient;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeWorkerConfigurationRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeWorkerConfigurationResponse;
import software.amazon.awssdk.services.kafkaconnect.model.ListTagsForResourceResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.Map;

public class ReadHandler extends BaseHandlerStd {
    private Logger logger;
    private final ExceptionTranslator exceptionTranslator;
    private final Translator translator;

    public ReadHandler() {
        this(new ExceptionTranslator(), new Translator());
    }

    /**
     * Constructor used for unit testing
     *
     * @param exceptionTranslator
     * @param translator
     */
    ReadHandler(final ExceptionTranslator exceptionTranslator, final Translator translator) {
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

        return proxy.initiate(
            "AWS-KafkaConnect-WorkerConfiguration::Read",
            proxyClient,
            request.getDesiredResourceState(),
            callbackContext)
            .translateToServiceRequest(translator::translateToReadRequest)
            .makeServiceCall(this::describeWorkerConfigurationWithTags)
            .done(responseModel -> ProgressEvent.defaultSuccessHandler(responseModel));
    }

    private ResourceModel describeWorkerConfigurationWithTags(
        final DescribeWorkerConfigurationRequest describeWorkerConfigurationRequest,
        final ProxyClient<KafkaConnectClient> proxyClient) {

        DescribeWorkerConfigurationResponse describeWorkerConfigurationResponse;
        Map<String, String> workerConfigurationTags;
        final String identifier = describeWorkerConfigurationRequest.workerConfigurationArn();
        final KafkaConnectClient kafkaConnectClient = proxyClient.client();

        try {
            describeWorkerConfigurationResponse =
                proxyClient.injectCredentialsAndInvokeV2(describeWorkerConfigurationRequest,
                    kafkaConnectClient::describeWorkerConfiguration);
        } catch (final AwsServiceException e) {
            throw exceptionTranslator.translateToCfnException(e, identifier);
        }

        try {
            final ListTagsForResourceResponse listTagsForResourceResponse = TagHelper
                .listTags(describeWorkerConfigurationRequest.workerConfigurationArn(), kafkaConnectClient, proxyClient);
            workerConfigurationTags = listTagsForResourceResponse.tags();

        } catch (final AwsServiceException e) {
            throw exceptionTranslator.translateToCfnException(e, identifier);

        }

        logger.log(
            String.format(
                "%s [%s] has successfully been read.",
                ResourceModel.TYPE_NAME,
                identifier));
        ResourceModel readResponse = translator.translateFromReadResponse(describeWorkerConfigurationResponse);
        readResponse.setTags(TagHelper.convertToSet(workerConfigurationTags));

        return readResponse;
    }
}
