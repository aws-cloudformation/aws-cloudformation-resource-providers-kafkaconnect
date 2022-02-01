package software.amazon.kafkaconnect.connector;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.kafkaconnect.KafkaConnectClient;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeConnectorRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeConnectorResponse;
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

        return proxy.initiate(
            "AWS-KafkaConnect-Connector::Read",
            proxyClient,
            request.getDesiredResourceState(),
            callbackContext)
            .translateToServiceRequest(translator::translateToReadRequest)

            .makeServiceCall(this::describeConnector)
            .done(responseModel -> ProgressEvent.defaultSuccessHandler(responseModel));
    }

    private ResourceModel describeConnector(
        final DescribeConnectorRequest describeConnectorRequest,
        final ProxyClient<KafkaConnectClient> proxyClient) {

        DescribeConnectorResponse describeConnectorResponse;
        final String identifier = describeConnectorRequest.connectorArn();
        final KafkaConnectClient kafkaConnectClient = proxyClient.client();

        try {
            describeConnectorResponse = proxyClient.injectCredentialsAndInvokeV2(describeConnectorRequest,
                kafkaConnectClient::describeConnector);
        } catch (final AwsServiceException e) {
            throw exceptionTranslator.translateToCfnException(e, identifier);
        }

        logger.log(
            String.format(
                "%s [%s] has successfully been read.",
                ResourceModel.TYPE_NAME,
                identifier
            )
        );
        return translator.translateFromReadResponse(describeConnectorResponse);
    }
}
