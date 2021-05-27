package io.basestar.schema;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestCycles {

    @Test
    void testCycles() throws IOException {

        final Namespace namespace = Namespace.load(TestViewSchema.class.getResource("cycles.yml"));
        final ViewSchema view = namespace.requireViewSchema("View");
        assertEquals(1, view.getFrom().schemas().size());
    }
}
