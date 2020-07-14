package io.basestar.schema;

import io.basestar.schema.exception.SchemaValidationException;
import io.basestar.util.Name;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestNamespace {

    private static final Name SOURCE = Name.of("source");

    @Test
    public void testQualified() throws IOException {

        final Namespace absoluteNamespace = Namespace.load(TestObjectSchema.class.getResource("qualified.yml"));

        final ObjectSchema absoluteSchema = absoluteNamespace.requireObjectSchema(SOURCE.with("A"));

        final Namespace dependencySchema = Namespace.from(absoluteSchema.dependencies());

        final Namespace dependencyExpandSchema = Namespace.from(absoluteSchema.dependencies(absoluteSchema.getExpand()));

        assertThrows(SchemaValidationException.class, () -> absoluteNamespace.relative(SOURCE));
//
//        assertThrows(SchemaValidationException.class, () -> dependencySchema.relative(SOURCE));
//
//        final Namespace relativeNamespace = dependencyExpandSchema.relative(SOURCE);
//
//        final ObjectSchema relativeSchema = relativeNamespace.requireObjectSchema(Name.of("A"));
//
//        assertEquals(2, relativeSchema.dependencies().size());
    }
}
