package io.basestar.schema.layout;

import com.google.common.collect.ImmutableMap;
import io.basestar.schema.Namespace;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseString;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestFlatStructLayout {

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
        final ObjectSchema schema = namespace.requireObjectSchema("Base");
        return new FlatStructLayout(schema);
    }

    @Test
    public void testSchema() throws IOException {

        final Layout layout = layout();

        final Map<String, Use<?>> result = layout.layout();

        assertEquals(10, result.size());
        assertEquals(UseString.DEFAULT.nullable(true), result.get("a__b1__c__d"));
        assertEquals(UseString.DEFAULT.nullable(true), result.get("a__b1__c__e"));
        assertEquals(UseString.DEFAULT, result.get("a__b2__c__d"));
        assertEquals(UseString.DEFAULT, result.get("a__b2__c__e"));
    }

    @Test
    public void testApply() throws IOException {

        final Layout layout = layout();

        final Map<String, Object> object = ImmutableMap.of(
                "a", a()
        );

        final Map<String, Object> result = layout.applyLayout(object);

        assertEquals(10, result.size());
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

        final Map<String, Object> result = layout.unapplyLayout(object);

        assertEquals(7, result.size());
        assertEquals(a(), result.get("a"));
    }
}
