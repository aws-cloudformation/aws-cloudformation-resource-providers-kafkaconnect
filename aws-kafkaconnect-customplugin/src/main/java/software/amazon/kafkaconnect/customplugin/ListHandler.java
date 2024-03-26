package software.amazon.kafkaconnect.customplugin;

import java.util.List;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.kafkaconnect.KafkaConnectClient;
import software.amazon.awssdk.services.kafkaconnect.model.ListCustomPluginsRequest;
import software.amazon.awssdk.services.kafkaconnect.model.ListCustomPluginsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class ListHandler extends BaseHandlerStd {
    private final ExceptionTranslator exceptionTranslator;
    private final Translator translator;

    public ListHandler() {
        this(new ExceptionTranslator(), new Translator());
    }

    /**
     * Constructor used for unit testing
     *
     * @param exceptionTranslator
     * @param translator
     */
    ListHandler(final ExceptionTranslator exceptionTranslator, final Translator translator) {
        this.exceptionTranslator = exceptionTranslator;
        this.translator = translator;
    }

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<KafkaConnectClient> proxyClient,
        final Logger logger) {

        final ListCustomPluginsRequest listCustomPluginsRequest =
            translator.translateToListRequest(request.getNextToken());

        final KafkaConnectClient kafkaConnectClient = proxyClient.client();

        ListCustomPluginsResponse listCustomPluginsResponse;

        try {
            listCustomPluginsResponse =
                proxyClient.injectCredentialsAndInvokeV2(
                    listCustomPluginsRequest, kafkaConnectClient::listCustomPlugins);
        } catch (final AwsServiceException e) {
            final String identifier = request.getAwsAccountId();
            throw exceptionTranslator.translateToCfnException(e, identifier);
        }

        final List<ResourceModel> models =
            translator.translateFromListResponse(listCustomPluginsResponse);
        final String nextToken = listCustomPluginsResponse.nextToken();

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
            .resourceModels(models)
            .nextToken(nextToken)
            .status(OperationStatus.SUCCESS)
            .build();
    }
}
