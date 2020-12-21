package io.basestar.schema;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestBucketing {

    @Test
    void testCompatibleBucketing() throws Exception {

        final Namespace namespace = Namespace.load(TestObjectSchema.class.getResource("bucketing.yml"));

        assertTrue(namespace.requireViewSchema("SimpleView").isCompatibleBucketing());
        assertFalse(namespace.requireViewSchema("DifferentOrderPointView").isCompatibleBucketing());
        assertFalse(namespace.requireViewSchema("DifferentCountPointView").isCompatibleBucketing());
        assertFalse(namespace.requireViewSchema("DifferentFunctionPointView").isCompatibleBucketing());
        assertTrue(namespace.requireViewSchema("CompatiblePointView").isCompatibleBucketing());
        assertTrue(namespace.requireViewSchema("NestedCompatiblePointView").isCompatibleBucketing());
    }
}
