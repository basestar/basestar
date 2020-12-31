package io.basestar.schema.use;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.schema.Instance;
import io.basestar.schema.Namespace;
import io.basestar.schema.ReferableSchema;
import io.basestar.schema.StructSchema;
import io.basestar.schema.util.Expander;
import io.basestar.util.Name;
import io.swagger.v3.oas.models.media.*;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.time.Instant;
import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.*;

class TestUse {

    @Test
    void testUseBoolean() throws Exception {

        testUse(UseBoolean.DEFAULT, null);
        testUse(UseBoolean.DEFAULT, true);
        testUse(UseBoolean.DEFAULT, "true", true);
        testUse(UseBoolean.DEFAULT, "false", false);
        testUse(UseBoolean.DEFAULT, 1, true);
        testUse(UseBoolean.DEFAULT, 0, false);
        testUse(UseBoolean.DEFAULT, 1.0, true);
        testUse(UseBoolean.DEFAULT, 0.0, false);
        assertTrue(UseBoolean.DEFAULT.openApi(ImmutableSet.of()) instanceof BooleanSchema);
        assertEquals(false, UseBoolean.DEFAULT.defaultValue());
    }

    @Test
    void testUseInteger() throws Exception {

        testUse(UseInteger.DEFAULT, null);
        testUse(UseInteger.DEFAULT, 1L);
        testUse(UseInteger.DEFAULT, "2", 2L);
        testUse(UseInteger.DEFAULT, 3.0, 3L);
        testUse(UseInteger.DEFAULT, 4, 4L);
        testUse(UseInteger.DEFAULT, true, 1L);
        testUse(UseInteger.DEFAULT, false, 0L);
        assertTrue(UseInteger.DEFAULT.openApi(ImmutableSet.of()) instanceof IntegerSchema);
        assertEquals(0L, UseInteger.DEFAULT.defaultValue());
    }

    @Test
    void testUseNumber() throws Exception {

        testUse(UseNumber.DEFAULT, null);
        testUse(UseNumber.DEFAULT, 1.5);
        testUse(UseNumber.DEFAULT, "2", 2.0);
        testUse(UseNumber.DEFAULT, 3.0, 3.0);
        testUse(UseNumber.DEFAULT, 4, 4.0);
        testUse(UseNumber.DEFAULT, true, 1.0);
        testUse(UseNumber.DEFAULT, false, 0.0);
        assertTrue(UseNumber.DEFAULT.openApi(ImmutableSet.of()) instanceof NumberSchema);
        assertEquals(0.0, UseNumber.DEFAULT.defaultValue());
    }

    @Test
    void testUseString() throws Exception {

        testUse(UseString.DEFAULT, null);
        testUse(UseString.DEFAULT, "test");
        assertTrue(UseString.DEFAULT.openApi(ImmutableSet.of()) instanceof StringSchema);
        assertEquals("", UseString.DEFAULT.defaultValue());
    }

    @Test
    void testUseDate() throws Exception {

        testUse(UseDate.DEFAULT, null);
        testUse(UseDate.DEFAULT, LocalDate.now());
        assertTrue(UseDate.DEFAULT.openApi(ImmutableSet.of()) instanceof DateSchema);
        assertEquals(LocalDate.ofEpochDay(0), UseDate.DEFAULT.defaultValue());
    }

    @Test
    void testUseDateTime() throws Exception {

        testUse(UseDateTime.DEFAULT, null);
        testUse(UseDateTime.DEFAULT, Instant.now());
        assertTrue(UseDateTime.DEFAULT.openApi(ImmutableSet.of()) instanceof DateTimeSchema);
        assertEquals(Instant.ofEpochMilli(0), UseDateTime.DEFAULT.defaultValue());
    }

    @Test
    void testUseArray() throws Exception {

        testUse(UseArray.DEFAULT, null);
        testUse(UseArray.from(UseString.DEFAULT), ImmutableList.of("a", "b", "c"));
        assertTrue(UseArray.DEFAULT.openApi(ImmutableSet.of()) instanceof ArraySchema);
        assertEquals(ImmutableList.of(), UseArray.DEFAULT.defaultValue());
    }

    @Test
    void testUseSet() throws Exception {

        testUse(UseSet.DEFAULT, null);
        testUse(UseSet.from(UseString.DEFAULT), ImmutableSet.of("a", "b", "c"));
        assertTrue(UseSet.DEFAULT.openApi(ImmutableSet.of()) instanceof ArraySchema);
        assertEquals(ImmutableSet.of(), UseSet.DEFAULT.defaultValue());
    }

    @Test
    void testUseMap() throws Exception {

        testUse(UseMap.DEFAULT, null);
        testUse(UseMap.from(UseString.DEFAULT), ImmutableMap.of("a", "1", "b", "2"));
        assertTrue(UseMap.DEFAULT.openApi(ImmutableSet.of()) instanceof MapSchema);
        assertEquals(ImmutableMap.of(), UseMap.from(UseString.DEFAULT).defaultValue());
    }

    @Test
    void testUseBinary() throws Exception {

        testUse(UseBinary.DEFAULT, null);
        testUse(UseBinary.DEFAULT, new byte[]{1, 2, 3});
        assertTrue(UseBinary.DEFAULT.openApi(ImmutableSet.of()) instanceof BinarySchema);
        assertArrayEquals(new byte[0], UseBinary.DEFAULT.defaultValue());
    }

    @Test
    void testUseStruct() throws Exception {

        final Namespace namespace = Namespace.load(TestUse.class.getResource("/schema/Petstore.yml"));
        final StructSchema schema = namespace.requireStructSchema("Address");
        final UseStruct use = UseStruct.from(schema, null);

        testUse(use, null);
        testUse(use, new Instance(ImmutableMap.of(
                "houseName", "MyHouse",
                "streetName", "MyStreet",
                "country", ReferableSchema.ref("GB"),
                "zip", "12345"
        )));
    }

    private <T> void testUse(final Use<T> use, final T init) throws Exception {

        testUse(use, init, init);
    }

    private <T> void testUse(final Use<T> use, final Object init, final T expect) throws Exception {

        final T created = use.create(init);
        assertTrue(use.areEqual(expect, created));

        final byte[] ser;
        try(final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final DataOutputStream dos = new DataOutputStream(baos)) {
            use.serialize(created, dos);
            ser = baos.toByteArray();
        }

        final T deser;
        try(final ByteArrayInputStream bais = new ByteArrayInputStream(ser);
            final DataInputStream dis = new DataInputStream(bais)) {
            deser = use.deserialize(dis);
        }
        assertTrue(use.areEqual(expect, deser));

        final T expanded = use.expand(Name.of(), created, Expander.noop(), ImmutableSet.of());
        assertSame(created, expanded);
    }
}
