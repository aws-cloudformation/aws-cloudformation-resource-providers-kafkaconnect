package software.amazon.kafkaconnect.connector;

import software.amazon.awssdk.services.kafkaconnect.KafkaConnectClient;
import software.amazon.cloudformation.LambdaWrapper;

import java.net.URI;

public class ClientBuilder {
    private static final String CN_PARTITION = "aws-cn";
    private static final String CN_SUFFIX = ".cn";
    private static final String SERVICE_ENDPOINT_TEMPLATE = "https://kafkaconnect.%s.amazonaws.com";

    private ClientBuilder() {
    }

    //It is recommended to use static HTTP client so less memory is consumed.
    public static KafkaConnectClient getClient(final String awsPartition, final String awsRegion) {
        return KafkaConnectClient
            .builder()
            .httpClient(LambdaWrapper.HTTP_CLIENT)
            .endpointOverride(getServiceEndpoint(awsPartition, awsRegion))
            .build();
    }

    private static URI getServiceEndpoint(final String partition, final String region) {
        // all cfn endpoints hit prod service endpoint in the same region
        final String serviceEndpoint = String.format(SERVICE_ENDPOINT_TEMPLATE, region);
        return URI.create(
            partition.equals(CN_PARTITION) ? serviceEndpoint + CN_SUFFIX : serviceEndpoint);
    }
}
