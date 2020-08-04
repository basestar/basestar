package io.basestar.schema.layout;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.Namespace;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseString;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestJsonCollectionLayout {

    private JsonCollectionLayout layout() throws IOException {

        final Namespace namespace = Namespace.load(TestFlatStructLayout.class.getResource("jsoncollection.yml"));
        final InstanceSchema schema = namespace.requireInstanceSchema("Base");
        return new JsonCollectionLayout(schema);
    }

    @Test
    public void testSchema() throws IOException {

        final Layout layout = layout();

        final Map<String, Use<?>> result = layout.layoutSchema(Collections.emptySet());

        assertEquals(9, result.size());
        assertEquals(UseString.DEFAULT.optional(true), result.get("a"));
        assertEquals(UseString.DEFAULT.optional(true), result.get("b"));
        assertEquals(UseString.DEFAULT, result.get("c"));
    }
    @Test
    public void testApply() throws IOException {

        final Layout layout = layout();

        final Map<String, Object> object = ImmutableMap.of(
                "a", ImmutableList.of("a", "b", "c"),
                "b", ImmutableMap.of("a", 1L, "b", 2L, "c", 3L),
                "c", ImmutableSet.of(true)
        );

        final Map<String, Object> result = layout.applyLayout(Collections.emptySet(), object);

        assertEquals(9, result.size());
        assertEquals("[\"a\",\"b\",\"c\"]", result.get("a"));
        assertEquals("{\"a\":1,\"b\":2,\"c\":3}", result.get("b"));
        assertEquals("[true]", result.get("c"));
    }

    @Test
    public void testUnapply() throws IOException {

        final Layout layout = layout();

        final Map<String, Object> object = ImmutableMap.of(
                "a", "[\"a\",\"b\",\"c\"]",
                "b", "{\"a\":1,\"b\":2,\"c\":3}",
                "c", "[true,true]"
        );

        final Map<String, Object> result = layout.unapplyLayout(Collections.emptySet(), object);

        assertEquals(9, result.size());
        assertEquals(ImmutableList.of("a", "b", "c"), result.get("a"));
        assertEquals(ImmutableMap.of("a", 1L, "b", 2L, "c", 3L), result.get("b"));
        assertEquals(ImmutableSet.of(true), result.get("c"));
    }
}
