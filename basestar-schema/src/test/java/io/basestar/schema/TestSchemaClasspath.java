package io.basestar.schema;

import io.basestar.schema.test.CustomSchema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestSchemaClasspath {

    static {
        System.setProperty(SchemaClasspath.Default.PROPERTY, "io.basestar.schema.test");
    }

    @Test
    void testObject() {

        assertEquals(SchemaClasspath.DEFAULT.classForId(ObjectSchema.Builder.TYPE), ObjectSchema.Builder.class);
        assertEquals(SchemaClasspath.DEFAULT.idForClass(ObjectSchema.Builder.class), ObjectSchema.Builder.TYPE);
    }

    @Test
    void testView() {

        assertEquals(SchemaClasspath.DEFAULT.classForId(ViewSchema.Builder.TYPE), ViewSchema.Builder.class);
        assertEquals(SchemaClasspath.DEFAULT.idForClass(ViewSchema.Builder.class), ViewSchema.Builder.TYPE);
    }

    @Test
    void testQuery() {

        assertEquals(SchemaClasspath.DEFAULT.classForId(QuerySchema.Builder.TYPE), QuerySchema.Builder.class);
        assertEquals(SchemaClasspath.DEFAULT.idForClass(QuerySchema.Builder.class), QuerySchema.Builder.TYPE);
    }

    @Test
    void testCustom() {

        assertEquals(SchemaClasspath.DEFAULT.classForId(CustomSchema.Builder.TYPE), CustomSchema.Builder.class);
        assertEquals(SchemaClasspath.DEFAULT.idForClass(CustomSchema.Builder.class), CustomSchema.Builder.TYPE);
    }
}
