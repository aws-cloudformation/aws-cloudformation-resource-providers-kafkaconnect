package software.amazon.kafkaconnect.connector;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.kafkaconnect.KafkaConnectClient;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeConnectorRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeConnectorResponse;
import software.amazon.awssdk.services.kafkaconnect.model.NotFoundException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

// Placeholder for the functionality that could be shared across Create/Read/Update/Delete/List Handlers

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {
    @Override
    public final ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final Logger logger) {

        return handleRequest(
            proxy,
            request,
            callbackContext != null ? callbackContext : new CallbackContext(),
            proxy.newProxy(() -> ClientBuilder.getClient(request.getAwsPartition(), request.getRegion())),
            logger
        );
    }

    protected abstract ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<KafkaConnectClient> proxyClient,
        final Logger logger);

    protected DescribeConnectorResponse runDescribeConnector(
        final DescribeConnectorRequest describeConnectorRequest,
        final ProxyClient<KafkaConnectClient> proxyClient,
        final String failureMessagePattern) {

        final KafkaConnectClient kafkaConnectClient = proxyClient.client();

        try {
            return proxyClient
                .injectCredentialsAndInvokeV2(describeConnectorRequest, kafkaConnectClient::describeConnector);
        } catch (final AwsServiceException e) {
            throw new CfnGeneralServiceException(
                String.format(
                    failureMessagePattern, ResourceModel.TYPE_NAME, e.getMessage()), e);
        }
    }

    protected DescribeConnectorResponse runDescribeConnectorWithNotFoundCatch(
        final DescribeConnectorRequest describeConnectorRequest,
        final ProxyClient<KafkaConnectClient> proxyClient,
        final String failureMessagePattern,
        final ExceptionTranslator exceptionTranslator) {

        final KafkaConnectClient kafkaConnectClient = proxyClient.client();

        try {
            return proxyClient
                .injectCredentialsAndInvokeV2(describeConnectorRequest, kafkaConnectClient::describeConnector);
        } catch (final NotFoundException e) {
            throw exceptionTranslator.translateToCfnException(e, describeConnectorRequest.connectorArn());
        } catch (final AwsServiceException e) {
            throw new CfnGeneralServiceException(
                String.format(failureMessagePattern, ResourceModel.TYPE_NAME, e.getMessage()), e);
        }
    }
}
