package io.basestar.jackson;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.ImmutableMap;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestBasestarFactory {

    final ObjectMapper objectMapper = new ObjectMapper(new BasestarFactory(new YAMLFactory()))
            .registerModule(BasestarModule.INSTANCE);

    @Test
    void testMerge() throws Exception {

        final Base base = objectMapper.readValue(TestBasestarFactory.class.getResource("base.yaml"), Base.class);
        assertEquals(new Base(ImmutableMap.of(
                "a", new Property("string"),
                "b", new Property("number"),
                "c", new Property("binary"),
                "d", new Property("integer"),
                "z", new Property("boolean")
        )), base);
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class Property {

        private String type;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class Base {

        private Map<String, Property> properties;
    }
}
