package software.amazon.kafkaconnect.workerconfiguration;

import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.retry.backoff.EqualJitterBackoffStrategy;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.services.kafkaconnect.KafkaConnectClient;
import software.amazon.cloudformation.LambdaWrapper;

import java.net.URI;
import java.time.Duration;

public class ClientBuilder {
    private static final String CN_PARTITION = "aws-cn";
    private static final String CN_SUFFIX = ".cn";
    private static final String SERVICE_ENDPOINT_TEMPLATE = "https://kafkaconnect.%s.amazonaws.com";

    private static final BackoffStrategy BACKOFF_THROTTLING_STRATEGY = EqualJitterBackoffStrategy.builder()
        .baseDelay(Duration.ofMillis(1200))
        .maxBackoffTime(Duration.ofSeconds(45))
        .build();

    private static final RetryPolicy RETRY_POLICY = RetryPolicy.builder()
        .numRetries(10)
        .retryCondition(RetryCondition.defaultRetryCondition())
        .backoffStrategy(BACKOFF_THROTTLING_STRATEGY)
        .throttlingBackoffStrategy(BACKOFF_THROTTLING_STRATEGY)
        .build();

    private ClientBuilder() {
    }

    public static KafkaConnectClient getClient(final String awsPartition, final String awsRegion) {
        return KafkaConnectClient
            .builder()
            .httpClient(LambdaWrapper.HTTP_CLIENT)
            .endpointOverride(getServiceEndpoint(awsPartition, awsRegion))
            .overrideConfiguration(ClientOverrideConfiguration.builder()
                .retryPolicy(RETRY_POLICY)
                .build())
            .build();
    }

    private static URI getServiceEndpoint(final String partition, final String region) {
        final String serviceEndpoint = String.format(SERVICE_ENDPOINT_TEMPLATE, region);
        return URI.create(
            partition.equals(CN_PARTITION) ? serviceEndpoint + CN_SUFFIX : serviceEndpoint);
    }
}
