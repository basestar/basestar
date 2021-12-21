package io.basestar.schema;

import io.basestar.schema.from.FromSql;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestQuerySchema {

    @Test
    void testQuerySchema() throws IOException {

        final Namespace namespace = Namespace.load(TestViewSchema.class.getResource("query.yml"));

        final QuerySchema schema = namespace.requireQuerySchema("PointQuery");
        assertEquals(2, schema.getArguments().size());
        assertTrue(schema.getFrom() instanceof FromSql);
        assertEquals(2, schema.getDeclaredProperties().size());
    }
}
