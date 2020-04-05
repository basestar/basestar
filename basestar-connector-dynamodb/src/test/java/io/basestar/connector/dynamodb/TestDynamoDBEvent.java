package io.basestar.connector.dynamodb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;

public class TestDynamoDBEvent {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    {
        objectMapper.registerModule(new JavaTimeModule());
    }

    @Test
    void testDeserialize() throws IOException {

        try(final InputStream is = getClass().getResourceAsStream("/streamEvent.json")) {

            final DynamoDBEvent event = objectMapper.readValue(is, DynamoDBEvent.class);

            //System.err.println(event);
        }
    }
}
