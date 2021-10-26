package io.basestar.schema;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.schema.from.FromSchema;
import io.basestar.util.Name;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestFunctionSchema {

    @Test
    public void testReplacedDefinition() {

        final String input = "SELECT * FROM @{test}";
        final String output = FunctionSchema.getReplacedDefinition(input, ImmutableMap.of("test", new FromSchema(
                ObjectSchema.builder().build(Name.of("test", "Object")), ImmutableSet.of()
        )), v -> v.getQualifiedName().toString());

        assertEquals("SELECT * FROM test.Object", output);
    }

    @Test
    public void testReplacedDefinitionQuoted() {

        final String input = "SELECT * FROM \"@{test}\"";
        final String output = FunctionSchema.getReplacedDefinition(input, ImmutableMap.of("test", new FromSchema(
                ObjectSchema.builder().build(Name.of("test", "Object")), ImmutableSet.of()
        )), v -> v.getQualifiedName().toString());

        assertEquals("SELECT * FROM test.Object", output);
    }
}
