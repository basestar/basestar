package io.basestar.schema;

import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Context;
import io.basestar.schema.encoding.FlatEncoding;
import io.basestar.schema.exception.SchemaValidationException;
import io.basestar.test.CsvUtils;
import io.basestar.util.ISO8601;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class TestInterfaceSchema {

    @Test
    void testCreate() throws Exception {

        final Namespace namespace = Namespace.load(TestInterfaceSchema.class.getResource("/schema/Petstore.yml"));
        final InterfaceSchema schema = namespace.requireInterfaceSchema("Pet");

        final List<Map<String, String>> inputs = CsvUtils.read(TestInterfaceSchema.class, "/data/Petstore/Cat.csv");

        final Instant now = ISO8601.now();
        final Map<String, Object> metadata = new HashMap<>();
        Instance.setSchema(metadata, Name.of("Cat"));
        Instance.setCreated(metadata, now);
        Instance.setUpdated(metadata, now);
        Instance.setVersion(metadata, 1L);

        final FlatEncoding encoding = new FlatEncoding();
        inputs.forEach(input -> {
            final Map<String, Object> decoded = Immutable.putAll(metadata, encoding.decode(input));
            final Instance instance = schema.create(decoded);
            assertNotNull(instance.get("catBreed"));
            final Instance evaluated = schema.evaluateProperties(Context.init(), instance, schema.getExpand());
            assertEquals("meow", evaluated.get("sound"));
            final Set<Constraint.Violation> violations = schema.validate(Context.init(), instance);
            assertEquals(ImmutableSet.of(), violations);
            final byte[] serialized;
            try(final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final DataOutputStream dos = new DataOutputStream(baos)) {
                schema.serialize(instance, dos);
                serialized = baos.toByteArray();
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
            final Map<String, Object> deserialized;
            try(final ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                final DataInputStream dis = new DataInputStream(bais)) {
                deserialized = ReferableSchema.deserialize(dis);
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
            assertEquals(instance, schema.create(deserialized));
        });
    }

    @Test
    void testConflictingNames() {
        SchemaValidationException schemaValidationException = assertThrows(SchemaValidationException.class,
                () -> Namespace.load(TestInterfaceSchema.class.getResource("conflictingNamesInterface.yml")));

        assertTrue(schemaValidationException.getMessage().contains("my-name"));
        assertTrue(schemaValidationException.getMessage().contains("MyName"));
        assertTrue(schemaValidationException.getMessage().contains("MySurname"));
        assertTrue(schemaValidationException.getMessage().contains("MY_SURNAME"));
        assertFalse(schemaValidationException.getMessage().contains("first"));
    }
}
