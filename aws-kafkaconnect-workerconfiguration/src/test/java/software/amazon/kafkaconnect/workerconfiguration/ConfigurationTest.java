package software.amazon.kafkaconnect.workerconfiguration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class ConfigurationTest extends AbstractTestBase {

    private Configuration configuration;

    @BeforeEach
    public void setup() {
        configuration = new Configuration();
    }

    @Test
    public void test_resourceDefinedTags_whenTagsAreNull() {
        final ResourceModel model = ResourceModel.builder().tags(null).build();

        final Map<String, String> response = configuration.resourceDefinedTags(model);

        assertThat(response).isNull();
    }

    @Test
    public void test_resourceDefinedTags_whenTagsAreNotNull() {
        final ResourceModel model = ResourceModel.builder().tags(TagHelper.convertToSet(TAGS)).build();

        final Map<String, String> response = configuration.resourceDefinedTags(model);

        assertThat(response).isNotNull();
        assertThat(response).isEqualTo(TagHelper.convertToMap(model.getTags()));
    }
}
