package io.basestar.avro;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.basestar.schema.Namespace;
import io.basestar.schema.ObjectSchema;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class TestAvroUtils {

    @Test
    void testSchema() throws Exception {

        final Namespace namespace = Namespace.load(TestAvroUtils.class.getResource("schema.yml"));
        final ObjectSchema schema = namespace.requireObjectSchema("A");

        final Schema avroSchema = AvroUtils.schema(schema);
        assertSame(Schema.Type.STRING, avroSchema.getField(ObjectSchema.ID).schema().getType());
        assertSame(Schema.Type.STRING, avroSchema.getField(ObjectSchema.SCHEMA).schema().getType());
        assertSame(Schema.Type.STRING, avroSchema.getField(ObjectSchema.CREATED).schema().getType());
        assertSame(Schema.Type.STRING, avroSchema.getField(ObjectSchema.UPDATED).schema().getType());
        assertSame(Schema.Type.STRING, avroSchema.getField(ObjectSchema.HASH).schema().getType());
        assertSame(Schema.Type.LONG, avroSchema.getField(ObjectSchema.VERSION).schema().getType());
        assertSame(Schema.Type.UNION, avroSchema.getField("array").schema().getType());
        assertSame(Schema.Type.UNION, avroSchema.getField("struct").schema().getType());
        assertSame(Schema.Type.LONG, avroSchema.getField("integer").schema().getType());
        assertSame(Schema.Type.BYTES, avroSchema.getField("binary").schema().getType());
    }

    @Test
    void testEncodeDecode() throws Exception {

        final Namespace namespace = Namespace.load(TestAvroUtils.class.getResource("schema.yml"));
        final ObjectSchema schema = namespace.requireObjectSchema("A");

        final Instant now = Instant.now();



        final Map<String, Object> before = schema.create(ImmutableMap.builder()
                .put("id", "a")
                .put("created", now)
                .put("updated", now)
                .put("version", 1L)
                .put("struct", ImmutableMap.builder()
                        .put("value", "Test")
                        .put("ref", ImmutableMap.builder()
                                .put("id", "b")
                                .put("created", now)
                                .put("updated", now)
                                .put("version", 1L)
                                .build())
                        .build())
                .put("integer", 10)
                .put("array", ImmutableList.of(
                        ImmutableMap.builder()
                                .put("id", "c")
                                .put("created", now)
                                .put("updated", now)
                                .put("version", 1L)
                                .build()
                ))
                .put("binary", new byte[]{1, 2, 3})
                .build(), schema.getExpand(), false);

        final Schema avroSchema = AvroUtils.schema(schema);
        final GenericRecord record = AvroUtils.encode(schema, avroSchema, before);
        final Map<String, Object> after = AvroUtils.decode(schema, avroSchema, record);

        assertEquals(before, after);

    }
}
