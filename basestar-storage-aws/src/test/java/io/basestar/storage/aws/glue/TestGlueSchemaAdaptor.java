package io.basestar.storage.aws.glue;

import io.basestar.schema.Namespace;
import io.basestar.storage.TestStorage;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;

public class TestGlueSchemaAdaptor {

    protected final Namespace namespace;

    protected TestGlueSchemaAdaptor() {

        try {
            this.namespace = Namespace.load(TestStorage.class.getResource("schema.yml"));
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    public void testAdaptSchema() {

        namespace.forEachReferableSchema((name, schema) -> {

            final GlueSchemaAdaptor adaptor = new GlueSchemaAdaptor(schema);
            System.err.println(adaptor.tableInput(schema.getName(), "file://"));
        });
    }
}
