package io.basestar.jackson.conf;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestPropsFactory {

    private final Map<String, String> env = ImmutableMap.of(
            "ENV1", "1",
            "ENV2", "2"
    );

    private final ObjectMapper objectMapper = new ObjectMapper(new SpringLikeJavaPropsFactory(
            new SpringLikeTextProcessor(env::get)
    ));


    @Test
    void testProps() throws IOException {

        final Props props = objectMapper.readValue(TestPropsFactory.class.getResourceAsStream("myapp.properties"), Props.class);
        assertEquals(new Props("test", new Env(1, 2, 20, null)), props);
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class Props {

        private String name;

        private Env env;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class Env {

        private Integer env1;

        private Integer env2;

        private Integer env3;

        private Integer env4;
    }
}
