package software.amazon.kafkaconnect.customplugin;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsResponse;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.services.kafkaconnect.KafkaConnectClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Credentials;
import software.amazon.cloudformation.proxy.LoggerProxy;
import software.amazon.cloudformation.proxy.ProxyClient;

public class AbstractTestBase {
    protected static final Credentials MOCK_CREDENTIALS =
        new Credentials("accessKey", "secretKey", "token");
    protected static final LoggerProxy logger = new LoggerProxy();

    protected static final Map<String, String> TAGS =
        new HashMap<String, String>() {
            {
                put("TEST_TAG1", "TEST_TAG_VALUE1");
                put("TEST_TAG2", "TEST_TAG_VALUE2");
            }
        };

    static ProxyClient<KafkaConnectClient> proxyStub(
        final AmazonWebServicesClientProxy proxy, final KafkaConnectClient kafkaConnectClient) {
        return new ProxyClient<KafkaConnectClient>() {
            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse> ResponseT injectCredentialsAndInvokeV2(
                RequestT request, Function<RequestT, ResponseT> requestFunction) {
                return proxy.injectCredentialsAndInvokeV2(request, requestFunction);
            }

            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse> CompletableFuture<ResponseT> injectCredentialsAndInvokeV2Async(
                RequestT request, Function<RequestT, CompletableFuture<ResponseT>> requestFunction) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse, IterableT extends SdkIterable<ResponseT>> IterableT injectCredentialsAndInvokeIterableV2(
                RequestT request, Function<RequestT, IterableT> requestFunction) {
                return proxy.injectCredentialsAndInvokeIterableV2(request, requestFunction);
            }

            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse> ResponseInputStream<ResponseT> injectCredentialsAndInvokeV2InputStream(
                RequestT requestT, Function<RequestT, ResponseInputStream<ResponseT>> function) {

                throw new UnsupportedOperationException();
            }

            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse> ResponseBytes<ResponseT> injectCredentialsAndInvokeV2Bytes(
                RequestT requestT, Function<RequestT, ResponseBytes<ResponseT>> function) {

                throw new UnsupportedOperationException();
            }

            @Override
            public KafkaConnectClient client() {
                return kafkaConnectClient;
            }
        };
    }
}
