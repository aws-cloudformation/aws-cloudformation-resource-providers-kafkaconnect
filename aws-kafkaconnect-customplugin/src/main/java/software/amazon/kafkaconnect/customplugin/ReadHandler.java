package software.amazon.kafkaconnect.customplugin;

import java.util.Map;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.kafkaconnect.KafkaConnectClient;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeCustomPluginRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeCustomPluginResponse;
import software.amazon.awssdk.services.kafkaconnect.model.ListTagsForResourceResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

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

        return proxy
            .initiate(
                "AWS-KafkaConnect-CustomPlugin::Read",
                proxyClient,
                request.getDesiredResourceState(),
                callbackContext)
            .translateToServiceRequest(translator::translateToReadRequest)
            .makeServiceCall(this::describeCustomPluginWithTags)
            .done(responseModel -> ProgressEvent.defaultSuccessHandler(responseModel));
    }

    private ResourceModel describeCustomPluginWithTags(
        final DescribeCustomPluginRequest describeCustomPluginRequest,
        final ProxyClient<KafkaConnectClient> proxyClient) {

        DescribeCustomPluginResponse describeCustomPluginResponse;
        Map<String, String> customPluginTags;
        final String identifier = describeCustomPluginRequest.customPluginArn();
        final KafkaConnectClient kafkaConnectClient = proxyClient.client();

        try {
            describeCustomPluginResponse =
                proxyClient.injectCredentialsAndInvokeV2(
                    describeCustomPluginRequest, kafkaConnectClient::describeCustomPlugin);
        } catch (final AwsServiceException e) {
            throw exceptionTranslator.translateToCfnException(e, identifier);
        }

        try {
            final ListTagsForResourceResponse listTagsForResourceResponse =
                TagHelper.listTags(
                    describeCustomPluginRequest.customPluginArn(), kafkaConnectClient, proxyClient);
            customPluginTags = listTagsForResourceResponse.tags();
        } catch (final AwsServiceException e) {
            throw exceptionTranslator.translateToCfnException(e, identifier);
        }

        logger.log(
            String.format("%s [%s] has successfully been read.", ResourceModel.TYPE_NAME, identifier));

        ResourceModel readResponse = translator.translateFromReadResponse(describeCustomPluginResponse);
        readResponse.setTags(TagHelper.convertToList(customPluginTags));
        return readResponse;
    }
}
