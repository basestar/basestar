package io.basestar.schema;

import org.junit.jupiter.api.Test;

import java.io.IOException;

public class TestConstraints {

    @Test
    public void testConstraints() throws IOException {

        final Namespace namespace = Namespace.load(TestViewSchema.class.getResource("constraints.yml"));

        final ObjectSchema schema = namespace.requireObjectSchema("Constrained");


        namespace.descriptor().yaml(System.out);
    }
}
