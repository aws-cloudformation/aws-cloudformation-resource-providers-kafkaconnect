package software.amazon.kafkaconnect.connector;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.kafkaconnect.KafkaConnectClient;
import software.amazon.awssdk.services.kafkaconnect.model.ConnectorSummary;
import software.amazon.awssdk.services.kafkaconnect.model.ListConnectorsRequest;
import software.amazon.awssdk.services.kafkaconnect.model.ListConnectorsResponse;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ListHandlerTest extends AbstractTestBase {

    @Mock
    private KafkaConnectClient kafkaConnectClient;

    @Mock
    private ExceptionTranslator exceptionTranslator;

    @Mock
    private Translator translator;

    private AmazonWebServicesClientProxy proxy;

    private ProxyClient<KafkaConnectClient> proxyClient;

    private ListHandler handler;

    @BeforeEach
    public void setup() {
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS,
            () -> Duration.ofSeconds(600).toMillis());
        proxyClient = proxyStub(proxy, kafkaConnectClient);
        handler = new ListHandler(exceptionTranslator, translator);
    }

    @AfterEach
    public void tear_down() {
        verifyNoMoreInteractions(kafkaConnectClient, exceptionTranslator, translator);
    }

    @Test
    public void handleRequest_success() {
        when(translator.translateToListRequest(TestData.NEXT_TOKEN_1)).thenReturn(TestData.LIST_CONNECTORS_REQUEST);
        when(proxyClient.injectCredentialsAndInvokeV2(
            TestData.LIST_CONNECTORS_REQUEST,
            kafkaConnectClient::listConnectors
        )).thenReturn(TestData.LIST_CONNECTORS_RESPONSE);
        when(translator.translateFromListResponse(TestData.LIST_CONNECTORS_RESPONSE))
            .thenReturn(TestData.CONNECTORS_MODELS);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler
            .handleRequest(proxy, TestData.RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, logger);

        assertThat(response).isEqualTo(TestData.SUCCESS_RESPONSE);
    }

    @Test
    public void handleRequest_throwsException_whenListConnectorsFails() {
        final AwsServiceException serviceException = AwsServiceException.builder().build();
        final CfnGeneralServiceException cfnException = new CfnGeneralServiceException(serviceException);
        when(translator.translateToListRequest(TestData.NEXT_TOKEN_1)).thenReturn(TestData.LIST_CONNECTORS_REQUEST);
        when(proxyClient
            .injectCredentialsAndInvokeV2(TestData.LIST_CONNECTORS_REQUEST, kafkaConnectClient::listConnectors))
            .thenThrow(serviceException);
        when(exceptionTranslator.translateToCfnException(serviceException, TestData.AWS_ACCOUNT_ID))
            .thenReturn(cfnException);

        final CfnGeneralServiceException exception =
            assertThrows(
                CfnGeneralServiceException.class, () -> handler.handleRequest(
                    proxy, TestData.RESOURCE_HANDLER_REQUEST, new CallbackContext(), proxyClient, logger)
            );

        assertThat(exception).isEqualTo(cfnException);
    }

    private static class TestData {
        private static final String NEXT_TOKEN_1 = "1234abcd";
        private static final String NEXT_TOKEN_2 = "56789efghi";
        private static final String AWS_ACCOUNT_ID = "1111111111";
        private static final String CONNECTOR_ARN_1 =
            "arn:aws:kafkaconnect:us-east-1:123456789:connector/unit-test-connector-1";
        private static final String CONNECTOR_ARN_2 =
            "arn:aws:kafkaconnect:us-east-1:123456789:connector/unit-test-connector-2";
        private static final String CONNECTOR_NAME_1 = "unit-test-connector-1";
        private static final String CONNECTOR_NAME_2 = "unit-test-connector-2";

        private static final ListConnectorsRequest LIST_CONNECTORS_REQUEST = ListConnectorsRequest
            .builder()
            .nextToken(NEXT_TOKEN_1)
            .build();
        private static final ResourceModel MODEL = ResourceModel.builder().build();
        private static final ResourceHandlerRequest<ResourceModel> RESOURCE_HANDLER_REQUEST =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(MODEL)
                .nextToken(NEXT_TOKEN_1)
                .awsAccountId(AWS_ACCOUNT_ID)
                .build();

        public static final List<ResourceModel> CONNECTORS_MODELS = Arrays.asList(
            testResourceModel(CONNECTOR_NAME_1, CONNECTOR_ARN_1),
            testResourceModel(CONNECTOR_NAME_2, CONNECTOR_ARN_2));

        private static final List<ConnectorSummary> CONNECTOR_SUMMARIES = Arrays.asList(
            testConnectorSummary(CONNECTOR_NAME_1, CONNECTOR_ARN_1),
            testConnectorSummary(CONNECTOR_NAME_2, CONNECTOR_ARN_2));

        private static final ProgressEvent<ResourceModel, CallbackContext> SUCCESS_RESPONSE =
            ProgressEvent.<ResourceModel, CallbackContext>builder()
                .resourceModels(CONNECTORS_MODELS)
                .nextToken(NEXT_TOKEN_2)
                .status(OperationStatus.SUCCESS)
                .build();

        private static final ListConnectorsResponse LIST_CONNECTORS_RESPONSE = ListConnectorsResponse
            .builder()
            .nextToken(TestData.NEXT_TOKEN_2)
            .connectors(TestData.CONNECTOR_SUMMARIES)
            .build();

        private static final ConnectorSummary testConnectorSummary(final String name, final String arn) {
            return ConnectorSummary
                .builder()
                .connectorName(name)
                .connectorArn(arn)
                .build();
        }

        private static ResourceModel testResourceModel(final String name, final String arn) {
            return ResourceModel
                .builder()
                .connectorName(name)
                .connectorArn(arn)
                .build();
        }
    }
}
