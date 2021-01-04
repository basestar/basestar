package io.basestar.schema.migration;

import com.google.common.collect.ImmutableMap;
import io.basestar.schema.Namespace;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.util.Widening;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TestMigration {

    @Test
    void testMigrate() throws Exception {

        final Namespace namespace = Namespace.load(TestMigration.class.getResource("schema.yml"));

        final ObjectSchema source = namespace.requireObjectSchema("Source");
        final ObjectSchema target1 = namespace.requireObjectSchema("Target1");
        final ObjectSchema target2 = namespace.requireObjectSchema("Target2");

        assertTrue(source.requiresMigration(target1, Widening.none()));
        assertFalse(source.requiresMigration(target2, Widening.none()));

        final Map<String, Object> a = source.create(ImmutableMap.of(
                "field1", "test",
                "field2", 14.0
        ));

        final Migration migration = Migration.load(TestMigration.class.getResource("migration.yml"));

        assertEquals(Operation.UPSERT, migration.operation(a));
        final Map<String, Object> migratedA = migration.migrate(target1, a);
        assertTrue(migratedA.get("field2") instanceof Long);

        final Map<String, Object> b = source.create(ImmutableMap.of(
                "deleted", true
        ));

        assertEquals(Operation.DELETE, migration.operation(b));
    }
}
