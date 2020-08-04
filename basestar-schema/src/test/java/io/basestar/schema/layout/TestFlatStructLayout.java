package io.basestar.schema.layout;

import com.google.common.collect.ImmutableMap;
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.Namespace;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseString;
import io.basestar.util.Name;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestFlatStructLayout {

    private final Set<Name> expand = Collections.emptySet();

    private Map<String, Object> a() {

        return ImmutableMap.of(
                "b1", ImmutableMap.of(
                        "c", ImmutableMap.of(
                                "d", "test1",
                                "e", "test2"
                        )
                ),
                "b2", ImmutableMap.of(
                        "c", ImmutableMap.of(
                                "d", "test3",
                                "e", "test4"
                        )
                )
        );
    }

    private Layout layout() throws IOException {

        final Namespace namespace = Namespace.load(TestFlatStructLayout.class.getResource("flatstruct.yml"));
        final InstanceSchema schema = namespace.requireInstanceSchema("Base");
        return new FlatStructLayout(schema);
    }

    @Test
    public void testSchema() throws IOException {

        final Layout layout = layout();

        final Map<String, Use<?>> result = layout.layoutSchema(expand);

        assertEquals(4, result.size());
        assertEquals(UseString.DEFAULT.optional(true), result.get("a__b1__c__d"));
        assertEquals(UseString.DEFAULT.optional(true), result.get("a__b1__c__e"));
        assertEquals(UseString.DEFAULT, result.get("a__b2__c__d"));
        assertEquals(UseString.DEFAULT, result.get("a__b2__c__e"));
    }

    @Test
    public void testApply() throws IOException {

        final Layout layout = layout();

        final Map<String, Object> object = ImmutableMap.of(
                "a", a()
        );

        final Map<String, Object> result = layout.applyLayout(expand, object);

        assertEquals(4, result.size());
        assertEquals("test1", result.get("a__b1__c__d"));
        assertEquals("test2", result.get("a__b1__c__e"));
        assertEquals("test3", result.get("a__b2__c__d"));
        assertEquals("test4", result.get("a__b2__c__e"));
    }

    @Test
    public void testUnapply() throws IOException {

        final Layout layout = layout();

        final Map<String, Object> object = ImmutableMap.of(
                "a__b1__c__d", "test1",
                "a__b1__c__e", "test2",
                "a__b2__c__d", "test3",
                "a__b2__c__e", "test4"
        );

        final Map<String, Object> result = layout.unapplyLayout(expand, object);

        assertEquals(1, result.size());
        assertEquals(a(), result.get("a"));
    }
}
