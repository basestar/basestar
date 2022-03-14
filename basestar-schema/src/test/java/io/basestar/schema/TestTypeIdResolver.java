package io.basestar.schema;

import io.basestar.schema.test.CustomSchema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestTypeIdResolver {

    static {
        System.setProperty(SchemaClasspath.Default.PROPERTY, "io.basestar.schema.test");
    }

    @Test
    void testObject() {

        assertEquals(ObjectSchema.Builder.class, SchemaClasspath.DEFAULT.classForId(ObjectSchema.Builder.TYPE));
        assertEquals(ObjectSchema.Builder.TYPE, new Schema.TypeIdResolver().idFromValue(ObjectSchema.builder()));
        assertEquals(ObjectSchema.Builder.TYPE, new Schema.TypeIdResolver().idFromValue(ObjectSchema.builder().build().descriptor()));
    }

    @Test
    void testView() {

        assertEquals(ViewSchema.Builder.class, SchemaClasspath.DEFAULT.classForId(ViewSchema.Builder.TYPE));
        assertEquals(ViewSchema.Builder.TYPE, new Schema.TypeIdResolver().idFromValue(ViewSchema.builder()));
        assertEquals(ViewSchema.Builder.TYPE, new Schema.TypeIdResolver().idFromValue(ViewSchema.builder().build().descriptor()));
    }

    @Test
    void testQuery() {

        assertEquals(QuerySchema.Builder.class, SchemaClasspath.DEFAULT.classForId(QuerySchema.Builder.TYPE));
        assertEquals(QuerySchema.Builder.TYPE, new Schema.TypeIdResolver().idFromValue(QuerySchema.builder()));
        assertEquals(QuerySchema.Builder.TYPE, new Schema.TypeIdResolver().idFromValue(QuerySchema.builder().build().descriptor()));
    }

    @Test
    void testCustom() {

        assertEquals(CustomSchema.Builder.class, SchemaClasspath.DEFAULT.classForId(CustomSchema.Builder.TYPE));
        assertEquals(CustomSchema.Builder.TYPE, new Schema.TypeIdResolver().idFromValue(CustomSchema.builder()));
        assertEquals(CustomSchema.Builder.TYPE, new Schema.TypeIdResolver().idFromValue(CustomSchema.builder().build().descriptor()));
    }
}
