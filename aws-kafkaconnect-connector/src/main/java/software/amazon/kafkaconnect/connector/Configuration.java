package software.amazon.kafkaconnect.connector;

import java.util.Map;

public class Configuration extends BaseConfiguration {

    public Configuration() {
        super("aws-kafkaconnect-connector.json");
    }

    /**
     * Providers should implement this method if their resource has a 'Tags' property to define resource-level tags
     * @return
     */
    public Map<String, String> resourceDefinedTags(final ResourceModel resourceModel) {
        if (resourceModel.getTags() == null) {
            return null;
        } else {
            return TagHelper.convertToMap(resourceModel.getTags());
        }
    }
}
