package io.basestar.storage.sql.mapping;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.schema.Instance;
import io.basestar.storage.sql.resolver.RecordResolver;
import io.basestar.util.Name;
import io.basestar.util.Pair;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestPropertyMapping {

    @Test
    void testSimpleMapping() {

        final Map<Name, Object> values = ImmutableMap.<Name, Object>builder()
                .put(Name.of("simpleString"), "test")
                .build();

        final PropertyMapping<String> mapping = PropertyMapping.simple(SQLDataType.LONGVARCHAR);

        final Name name = Name.of("simpleString");

        assertEquals("test", mapping.fromRecord(name, RecordResolver.from(values), ImmutableSet.of()));
        assertEquals(ImmutableList.of(
                Pair.of(
                        Name.of("simpleString"),
                        DSL.inline("test")
                )
        ), mapping.emit(name, "test"));
    }

    @Test
    void testFlattenedStructMapping() {

        final Map<Name, Object> values = ImmutableMap.<Name, Object>builder()
                .put(Name.of("simpleStruct", "key"), "test1")
                .put(Name.of("simpleStruct", "value"), "test2")
                .build();

        final Instance expected = new Instance(ImmutableMap.<String, Object>builder()
                .put("key", "test1")
                .put("value", "test2")
                .build());

        final Name name = Name.of("simpleStruct");

        final PropertyMapping<Instance> mapping = PropertyMapping.flattenedStruct(ImmutableMap.of(
                "key", PropertyMapping.simple(SQLDataType.LONGVARCHAR),
                "value", PropertyMapping.simple(SQLDataType.LONGVARCHAR)));

        assertEquals(expected, mapping.fromRecord(name, RecordResolver.from(values), ImmutableSet.of()));
        assertEquals(ImmutableList.of(
                Pair.of(
                        Name.of("simpleStruct", "key"),
                        DSL.inline("test1")
                ),
                Pair.of(
                        Name.of("simpleStruct", "value"),
                        DSL.inline("test2")
                )
        ), mapping.emit(name, expected));
    }

    @Test
    void testFlattenedRefMapping() {

        final Map<Name, Object> values = ImmutableMap.<Name, Object>builder()
                .put(Name.of("simpleRef", "id"), "test1")
                .put(Name.of("simpleRef", "version"), 1L)
                .build();

        final Instance expected = new Instance(ImmutableMap.<String, Object>builder()
                .put("id", "test1")
                .put("version", 1L)
                .build());

        final Name name = Name.of("simpleRef");

        final PropertyMapping<Instance> mapping = PropertyMapping.flattenedStruct(ImmutableMap.of(
                "id", PropertyMapping.simple(SQLDataType.LONGVARCHAR),
                "version", PropertyMapping.simple(SQLDataType.BIGINT)));

        assertEquals(expected, mapping.fromRecord(name, RecordResolver.from(values), ImmutableSet.of()));
        assertEquals(ImmutableList.of(
                Pair.of(
                        Name.of("simpleRef", "id"),
                        DSL.inline("test1")
                ),
                Pair.of(
                        Name.of("simpleRef", "version"),
                        DSL.inline(1L)
                )
        ), mapping.emit(name, expected));
    }
}
