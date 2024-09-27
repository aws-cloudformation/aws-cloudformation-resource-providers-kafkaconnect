package software.amazon.kafkaconnect.customplugin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.kafkaconnect.KafkaConnectClient;
import software.amazon.awssdk.services.kafkaconnect.model.BadRequestException;
import software.amazon.awssdk.services.kafkaconnect.model.CustomPluginState;
import software.amazon.awssdk.services.kafkaconnect.model.DeleteCustomPluginRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DeleteCustomPluginResponse;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeCustomPluginRequest;
import software.amazon.awssdk.services.kafkaconnect.model.DescribeCustomPluginResponse;
import software.amazon.awssdk.services.kafkaconnect.model.NotFoundException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.exceptions.CfnNotStabilizedException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

@ExtendWith(MockitoExtension.class)
public class DeleteHandlerTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<KafkaConnectClient> proxyClient;

    @Mock
    private KafkaConnectClient kafkaConnectClient;

    @Mock
    private ExceptionTranslator exceptionTranslator;

    @Mock
    private Translator translator;

    private DeleteHandler handler;

    @BeforeEach
    public void setup() {
        proxy =
            new AmazonWebServicesClientProxy(
                logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        proxyClient = proxyStub(proxy, kafkaConnectClient);
        handler = new DeleteHandler(exceptionTranslator, translator);
    }

    @AfterEach
    public void tear_down() {
        verify(kafkaConnectClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(kafkaConnectClient, exceptionTranslator, translator);
    }

    @Test
    public void test_handleRequest_success() {
        final DescribeCustomPluginRequest describeCustomPluginRequest =
            TestData.createDescribeCustomPluginRequest();
        final ResourceModel resourceModel = TestData.createResourceModel();

        when(translator.translateToReadRequest(resourceModel)).thenReturn(describeCustomPluginRequest);
        when(proxyClient.injectCredentialsAndInvokeV2(
            describeCustomPluginRequest, kafkaConnectClient::describeCustomPlugin))
                .thenReturn(
                    TestData.createDescribeCustomPluginResponse(CustomPluginState.ACTIVE)) // First call to
                // validate resource
                .thenReturn(
                    TestData.createDescribeCustomPluginResponse(
                        CustomPluginState.DELETING)) // Second call to
                // stabalize
                // resource
                .thenThrow(NotFoundException.class); // Third call to finalise deletion of the resource
        when(kafkaConnectClient.deleteCustomPlugin(any(DeleteCustomPluginRequest.class)))
            .thenReturn(TestData.createDeleteCustomPluginResponse());
        when(translator.translateToDeleteRequest(resourceModel))
            .thenReturn(TestData.createDeleteCustomPluginRequest());

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(
                proxy,
                TestData.createResourceHandlerRequest(resourceModel),
                new CallbackContext(),
                proxyClient,
                logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(kafkaConnectClient, times(1)).deleteCustomPlugin(any(DeleteCustomPluginRequest.class));
        verify(kafkaConnectClient, times(3))
            .describeCustomPlugin(any(DescribeCustomPluginRequest.class));
    }

    @Test
    public void test_handleRequest_failure_dueToBadRequest() {
        final DescribeCustomPluginRequest describeCustomPluginRequest =
            TestData.createDescribeCustomPluginRequest();
        final ResourceModel resourceModel = TestData.createResourceModel();
        final BadRequestException serviceException = BadRequestException.builder().build();
        final CfnInvalidRequestException cfnException =
            new CfnInvalidRequestException(serviceException);

        when(translator.translateToReadRequest(resourceModel)).thenReturn(describeCustomPluginRequest);
        when(proxyClient.injectCredentialsAndInvokeV2(
            describeCustomPluginRequest, kafkaConnectClient::describeCustomPlugin))
                .thenThrow(serviceException);
        when(exceptionTranslator.translateToCfnException(serviceException, TestData.CUSTOM_PLUGIN_ARN))
            .thenReturn(cfnException);

        assertThrows(
            CfnInvalidRequestException.class,
            () -> handler.handleRequest(
                proxy,
                TestData.createResourceHandlerRequest(resourceModel),
                new CallbackContext(),
                proxyClient,
                logger));

        verify(kafkaConnectClient, times(0)).deleteCustomPlugin(any(DeleteCustomPluginRequest.class));
        verify(kafkaConnectClient, times(1))
            .describeCustomPlugin(any(DescribeCustomPluginRequest.class));
    }

    @Test
    public void test_handleRequest_failure_dueToEmptyCustomPluginArn() {
        final DescribeCustomPluginRequest describeCustomPluginRequest =
            DescribeCustomPluginRequest.builder().customPluginArn(null).build();
        final ResourceModel resourceModel = TestData.createResourceModel();

        when(translator.translateToReadRequest(resourceModel)).thenReturn(describeCustomPluginRequest);

        assertThrows(
            CfnNotFoundException.class,
            () -> handler.handleRequest(
                proxy,
                TestData.createResourceHandlerRequest(resourceModel),
                new CallbackContext(),
                proxyClient,
                logger));

        verify(kafkaConnectClient, times(0)).deleteCustomPlugin(any(DeleteCustomPluginRequest.class));
        verify(kafkaConnectClient, times(0))
            .describeCustomPlugin(any(DescribeCustomPluginRequest.class));
    }

    @Test
    public void test_handleRequest_failure_dueToCustomPluginNotFound() {
        final DescribeCustomPluginRequest describeCustomPluginRequest = TestData.createDescribeCustomPluginRequest();
        final ResourceModel resourceModel = TestData.createResourceModel();
        final NotFoundException serviceException = NotFoundException.builder().build();
        final CfnNotFoundException cfnException = new CfnNotFoundException(serviceException);

        when(translator.translateToReadRequest(resourceModel)).thenReturn(describeCustomPluginRequest);
        when(proxyClient.injectCredentialsAndInvokeV2(
            describeCustomPluginRequest, kafkaConnectClient::describeCustomPlugin))
                .thenThrow(serviceException);
        when(exceptionTranslator.translateToCfnException(serviceException, TestData.CUSTOM_PLUGIN_ARN))
            .thenReturn(cfnException);

        assertThrows(
            CfnNotFoundException.class,
            () -> handler.handleRequest(
                proxy,
                TestData.createResourceHandlerRequest(resourceModel),
                new CallbackContext(),
                proxyClient,
                logger));

        verify(kafkaConnectClient, times(0)).deleteCustomPlugin(any(DeleteCustomPluginRequest.class));
        verify(kafkaConnectClient, times(1))
            .describeCustomPlugin(any(DescribeCustomPluginRequest.class));
    }

    @Test
    public void test_handleRequest_failure_alreadyDeleted() {
        final DescribeCustomPluginRequest describeCustomPluginRequest =
            TestData.createDescribeCustomPluginRequest();
        final ResourceModel resourceModel = TestData.createResourceModel();
        final NotFoundException serviceException = NotFoundException.builder().build();
        final CfnNotFoundException cfnException = new CfnNotFoundException(serviceException);

        when(translator.translateToReadRequest(resourceModel)).thenReturn(describeCustomPluginRequest);
        when(proxyClient.injectCredentialsAndInvokeV2(
            describeCustomPluginRequest, kafkaConnectClient::describeCustomPlugin))
                .thenReturn(TestData.createDescribeCustomPluginResponse(CustomPluginState.ACTIVE));
        when(kafkaConnectClient.deleteCustomPlugin(any(DeleteCustomPluginRequest.class)))
            .thenThrow(serviceException);
        when(exceptionTranslator.translateToCfnException(serviceException, TestData.CUSTOM_PLUGIN_ARN))
            .thenReturn(cfnException);
        when(translator.translateToDeleteRequest(resourceModel))
            .thenReturn(TestData.createDeleteCustomPluginRequest());

        assertThrows(
            CfnNotFoundException.class,
            () -> handler.handleRequest(
                proxy,
                TestData.createResourceHandlerRequest(resourceModel),
                new CallbackContext(),
                proxyClient,
                logger));

        verify(kafkaConnectClient, times(1)).deleteCustomPlugin(any(DeleteCustomPluginRequest.class));
        verify(kafkaConnectClient, times(1))
            .describeCustomPlugin(any(DescribeCustomPluginRequest.class));
    }

    @Test
    public void test_handleRequest_failure_stabilizeHandlesServiceExceptions() {
        final DescribeCustomPluginRequest describeCustomPluginRequest =
            TestData.createDescribeCustomPluginRequest();
        final ResourceModel resourceModel = TestData.createResourceModel();

        when(translator.translateToReadRequest(resourceModel)).thenReturn(describeCustomPluginRequest);
        when(proxyClient.injectCredentialsAndInvokeV2(
            describeCustomPluginRequest, kafkaConnectClient::describeCustomPlugin))
                .thenReturn(TestData.createDescribeCustomPluginResponse(CustomPluginState.ACTIVE))
                .thenThrow(AwsServiceException.builder().build());
        when(kafkaConnectClient.deleteCustomPlugin(any(DeleteCustomPluginRequest.class)))
            .thenReturn(TestData.createDeleteCustomPluginResponse());
        when(translator.translateToDeleteRequest(resourceModel))
            .thenReturn(TestData.createDeleteCustomPluginRequest());

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(
                proxy,
                TestData.createResourceHandlerRequest(resourceModel),
                new CallbackContext(),
                proxyClient,
                logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();

        verify(kafkaConnectClient, times(1)).deleteCustomPlugin(any(DeleteCustomPluginRequest.class));
        verify(kafkaConnectClient, times(2))
            .describeCustomPlugin(any(DescribeCustomPluginRequest.class));
        verify(exceptionTranslator, times(1))
            .translateToCfnException(any(AwsServiceException.class), eq(TestData.CUSTOM_PLUGIN_ARN));
    }

    @Test
    public void test_handleRequest_failure_stabilizeFails_dueToUnexpectedState() {
        final DescribeCustomPluginRequest describeCustomPluginRequest =
            TestData.createDescribeCustomPluginRequest();
        final ResourceModel resourceModel = TestData.createResourceModel();

        when(translator.translateToReadRequest(resourceModel)).thenReturn(describeCustomPluginRequest);
        when(proxyClient.injectCredentialsAndInvokeV2(
            describeCustomPluginRequest, kafkaConnectClient::describeCustomPlugin))
                .thenReturn(TestData.createDescribeCustomPluginResponse(CustomPluginState.ACTIVE))
                .thenReturn(
                    TestData.createDescribeCustomPluginResponse(CustomPluginState.UNKNOWN_TO_SDK_VERSION));
        when(kafkaConnectClient.deleteCustomPlugin(any(DeleteCustomPluginRequest.class)))
            .thenReturn(TestData.createDeleteCustomPluginResponse());
        when(translator.translateToDeleteRequest(resourceModel))
            .thenReturn(TestData.createDeleteCustomPluginRequest());

        assertThrows(
            CfnNotStabilizedException.class,
            () -> handler.handleRequest(
                proxy,
                TestData.createResourceHandlerRequest(resourceModel),
                new CallbackContext(),
                proxyClient,
                logger));

        verify(kafkaConnectClient, times(1)).deleteCustomPlugin(any(DeleteCustomPluginRequest.class));
        verify(kafkaConnectClient, times(2))
            .describeCustomPlugin(any(DescribeCustomPluginRequest.class));
    }

    private static class TestData {
        private static final String CUSTOM_PLUGIN_ARN =
            "arn:aws:kafkaconnect:us-east-1:123456789:custom-plugin/unit-test-custom-plugin";

        private static ResourceModel createResourceModel() {
            return ResourceModel.builder().customPluginArn(CUSTOM_PLUGIN_ARN).build();
        }

        private static ResourceHandlerRequest<ResourceModel> createResourceHandlerRequest(
            ResourceModel resourceModel) {
            return ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(resourceModel)
                .build();
        }

        private static DescribeCustomPluginRequest createDescribeCustomPluginRequest() {
            return DescribeCustomPluginRequest.builder().customPluginArn(CUSTOM_PLUGIN_ARN).build();
        }

        private static DescribeCustomPluginResponse createDescribeCustomPluginResponse(
            CustomPluginState state) {
            return DescribeCustomPluginResponse.builder()
                .customPluginArn(CUSTOM_PLUGIN_ARN)
                .customPluginState(state)
                .build();
        }

        private static DeleteCustomPluginRequest createDeleteCustomPluginRequest() {
            return DeleteCustomPluginRequest.builder().customPluginArn(CUSTOM_PLUGIN_ARN).build();
        }

        private static DeleteCustomPluginResponse createDeleteCustomPluginResponse() {
            return DeleteCustomPluginResponse.builder().customPluginArn(CUSTOM_PLUGIN_ARN).build();
        }
    }
}
