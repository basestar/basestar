package io.basestar.schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseArray;
import io.basestar.schema.use.UseMap;
import io.basestar.schema.use.UseString;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestUse {

    @Test
    public void deserialize() throws IOException {

        final Use<?> string = new ObjectMapper().readValue("\"string\"", Use.class);
        assertEquals(UseString.DEFAULT, string);

        final Use<?> array = new ObjectMapper().readValue("{\"array\": \"string\"}", Use.class);
        assertEquals(new UseArray<>(UseString.DEFAULT), array);

        final Use<?> map = new ObjectMapper().readValue("{\"map\": \"string\"}", Use.class);
        assertEquals(new UseMap<>(UseString.DEFAULT), map);
    }
}
