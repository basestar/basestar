package io.basestar.storage.aws.glue;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.basestar.schema.Namespace;
import io.basestar.storage.TestStorage;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.model.TableInput;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestGlueSchemaAdaptor {

    protected final ObjectMapper objectMapper = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

    protected final Namespace namespace;

    protected TestGlueSchemaAdaptor() {

        try {
            this.namespace = Namespace.load(TestStorage.class.getResource("schema.yml"));
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testAdaptSchema() throws Exception {

        final Map<String, TableInput.Builder> tableInputs = new HashMap<>();

        namespace.forEachReferableSchema((name, schema) -> {

            final GlueSchemaAdaptor adaptor = new GlueSchemaAdaptor(schema);
            final TableInput tableInput = adaptor.tableInput(schema.getName(), "file://");
            tableInputs.put(name.toString(), tableInput.toBuilder());
        });

        final Map<String, Object> expected = (Map<String, Object>)objectMapper.readValue(TestGlueSchemaAdaptor.class.getResourceAsStream("/gluedefinitions.json"), Map.class);
        final Map<String, Object> actual = objectMapper.convertValue(tableInputs, Map.class);

        assertEquals(expected, actual);
    }
}
