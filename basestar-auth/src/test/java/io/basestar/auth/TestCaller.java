package io.basestar.auth;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.basestar.util.Name;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class TestCaller {

    @Test
    public void testDeserialize() throws IOException {

        final String json = "{\"super\":false, \"schema\":\"User\", \"id\":\"matt\"}";

        final Caller caller = new ObjectMapper().readValue(json, Caller.class);

        assertFalse(caller.isSuper());
        assertEquals(Name.of("User"), caller.getSchema());
        assertEquals("matt", caller.getId());
    }
}
