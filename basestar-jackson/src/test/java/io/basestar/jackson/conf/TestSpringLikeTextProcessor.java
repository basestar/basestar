package io.basestar.jackson.conf;

import io.basestar.util.Immutable;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class TestSpringLikeTextProcessor {

    @Test
    void testRegexQuoting() {

        final Map<String, String> args = Immutable.map("NAME", "$value");
        final SpringLikeTextProcessor processor = new SpringLikeTextProcessor(args::get);
        assertEquals("name=$value", processor.apply("name=${NAME}"));
    }
}
