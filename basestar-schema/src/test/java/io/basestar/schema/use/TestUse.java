package io.basestar.schema.use;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.schema.*;
import io.basestar.schema.util.Expander;
import io.basestar.secret.Secret;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Page;
import io.leangen.geantyref.GenericTypeReflector;
import io.swagger.v3.oas.models.media.*;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.lang.reflect.Type;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class TestUse {

    private static final List<Use<?>> SCALARS = ImmutableList.of(UseBoolean.DEFAULT,
            UseInteger.DEFAULT, UseNumber.DEFAULT, UseString.DEFAULT, UseDate.DEFAULT,
            UseDateTime.DEFAULT, UseBinary.DEFAULT, UseSecret.DEFAULT);

    private Namespace namespace() throws IOException {

        return Namespace.load(TestUse.class.getResource("/schema/Petstore.yml"));
    }

    @Test
    void testScalarSyntax() {

        SCALARS.forEach(use -> {

            assertEquals(use, Use.fromConfig(use.toConfig()));
            assertEquals(UseOptional.from(use), Use.fromConfig(use.toConfig(true)));
            assertEquals(use, Use.fromConfig(use.toString()));
            assertEquals(use, Use.fromConfig(Immutable.map(use.toString(), null)));
        });
    }

    @Test
    void testArraySyntax() {

        final UseArray<String> use = UseArray.from(UseString.DEFAULT);
        assertEquals(use, Use.fromConfig(use.toConfig()));
        assertEquals(UseOptional.from(use), Use.fromConfig(use.toConfig(true)));
        assertEquals(use, Use.fromConfig(Immutable.map(UseArray.NAME, UseString.NAME)));
        assertEquals(use, Use.fromConfig(Immutable.map(UseArray.NAME, Immutable.map("type", UseString.NAME))));
    }

    @Test
    void testSetSyntax() {

        final UseSet<String> use = UseSet.from(UseString.DEFAULT);
        assertEquals(use, Use.fromConfig(use.toConfig()));
        assertEquals(UseOptional.from(use), Use.fromConfig(use.toConfig(true)));
        assertEquals(use, Use.fromConfig(Immutable.map(UseSet.NAME, UseString.NAME)));
        assertEquals(use, Use.fromConfig(Immutable.map(UseSet.NAME, Immutable.map("type", UseString.NAME))));
    }

    @Test
    void testMapSyntax() {

        final UseMap<String> use = UseMap.from(UseString.DEFAULT);
        assertEquals(use, Use.fromConfig(use.toConfig()));
        assertEquals(UseOptional.from(use), Use.fromConfig(use.toConfig(true)));
        assertEquals(use, Use.fromConfig(Immutable.map(UseMap.NAME, UseString.NAME)));
        assertEquals(use, Use.fromConfig(Immutable.map(UseMap.NAME, Immutable.map("type", UseString.NAME))));
    }

    @Test
    void testStructResolve() throws Exception {

        final Namespace namespace = namespace();

        final Use<?> use = Use.fromConfig("Address");
        assertEquals(UseStruct.from(namespace.requireStructSchema("Address"), null), use.resolve(namespace));
    }

    @Test
    void testRefResolve() throws Exception {

        final Namespace namespace = namespace();

        final Use<?> use = Use.fromConfig("Pet");
        assertEquals(UseRef.from(namespace.requireReferableSchema("Pet"), null), use.resolve(namespace));
    }

    @Test
    void testVersionedRefResolve() throws Exception {

        final Namespace namespace = namespace();

        final Use<?> use = Use.fromConfig(Immutable.map("Pet", Immutable.map("versioned", true)));
        assertEquals(new UseRef(namespace.requireReferableSchema("Pet"), true), use.resolve(namespace));
    }

    @Test
    void testUseBoolean() throws Exception {

        final UseBoolean use = UseBoolean.DEFAULT;
        testUse(use, null);
        testUse(use, true);
        testUse(use, "true", true);
        testUse(use, "false", false);
        testUse(use, 1, true);
        testUse(use, 0, false);
        testUse(use, 1.0, true);
        testUse(use, 0.0, false);
        assertTrue(use.openApi(ImmutableSet.of()) instanceof BooleanSchema);
        assertEquals(false, use.defaultValue());
        assertEquals(Boolean.class, use.javaType());
        assertEquals(use, Use.fromJavaType(Boolean.class));
        assertEquals("null", use.toString(null));
        assertEquals("true", use.toString(true));
        assertEquals("false", use.toString(false));
    }

    @Test
    void testUseInteger() throws Exception {

        final UseInteger use = UseInteger.DEFAULT;
        testUse(use, null);
        testUse(use, 1L);
        testUse(use, "2", 2L);
        testUse(use, 3.0, 3L);
        testUse(use, 4, 4L);
        testUse(use, true, 1L);
        testUse(use, false, 0L);
        assertTrue(use.openApi(ImmutableSet.of()) instanceof IntegerSchema);
        assertEquals(0L, use.defaultValue());
        assertEquals(Long.class, use.javaType());
        assertEquals(use, Use.fromJavaType(Long.class));
        assertEquals(use, Use.fromJavaType(Byte.class));
        assertEquals(use, Use.fromJavaType(Short.class));
        assertEquals(use, Use.fromJavaType(Integer.class));
        assertEquals("null", use.toString(null));
        assertEquals("2", use.toString(2L));
    }

    @Test
    void testUseNumber() throws Exception {

        final UseNumber use = UseNumber.DEFAULT;
        testUse(use, null);
        testUse(use, 1.5);
        testUse(use, "2", 2.0);
        testUse(use, 3.0, 3.0);
        testUse(use, 4, 4.0);
        testUse(use, true, 1.0);
        testUse(use, false, 0.0);
        assertTrue(use.openApi(ImmutableSet.of()) instanceof NumberSchema);
        assertEquals(0.0, use.defaultValue());
        assertEquals(Double.class, use.javaType());
        assertEquals(use, Use.fromJavaType(Double.class));
        assertEquals(use, Use.fromJavaType(Float.class));
        assertEquals("null", use.toString(null));
        assertEquals("2.0", use.toString(2.0));
    }

    @Test
    void testUseString() throws Exception {

        final UseString use = UseString.DEFAULT;
        testUse(use, null);
        testUse(use, "test");
        assertTrue(use.openApi(ImmutableSet.of()) instanceof StringSchema);
        assertEquals("", use.defaultValue());
        assertEquals(String.class, use.javaType());
        assertEquals(use, Use.fromJavaType(String.class));
        assertEquals("null", use.toString(null));
        assertEquals("test", use.toString("test"));
    }

    @Test
    void testUseDate() throws Exception {

        final UseDate use = UseDate.DEFAULT;
        testUse(use, null);
        testUse(use, LocalDate.now());
        assertTrue(use.openApi(ImmutableSet.of()) instanceof DateSchema);
        assertEquals(LocalDate.ofEpochDay(0), use.defaultValue());
        assertEquals(LocalDate.class, use.javaType());
        assertEquals(use, Use.fromJavaType(LocalDate.class));
        assertEquals("null", use.toString(null));
        assertEquals("2020-01-01", use.toString(LocalDate.parse("2020-01-01")));
    }

    @Test
    void testUseDateTime() throws Exception {

        final UseDateTime use = UseDateTime.DEFAULT;
        testUse(use, null);
        testUse(use, Instant.now());
        assertTrue(use.openApi(ImmutableSet.of()) instanceof DateTimeSchema);
        assertEquals(Instant.ofEpochMilli(0), use.defaultValue());
        assertEquals(Instant.class, use.javaType());
        assertEquals(use, Use.fromJavaType(Instant.class));
        assertEquals("null", use.toString(null));
        assertEquals("2020-01-01T00:00:00.000Z", use.toString(Instant.parse("2020-01-01T00:00:00Z")));
    }

    @Test
    void testUseBinary() throws Exception {

        final UseBinary use = UseBinary.DEFAULT;
        testUse(use, null);
        testUse(use, new byte[]{1, 2, 3});
        assertTrue(use.openApi(ImmutableSet.of()) instanceof BinarySchema);
        assertArrayEquals(new byte[0], use.defaultValue());
        assertEquals(byte[].class, use.javaType());
        assertEquals(use, Use.fromJavaType(byte[].class));
        assertEquals("null", use.toString(null));
        assertEquals("AQ==", use.toString(new byte[]{1}));
    }

    @Test
    void testUseSecret() throws Exception {

        final UseSecret use = UseSecret.DEFAULT;
        testUse(use, null);
        testUse(use, Secret.encrypted("AQ=="));
        assertTrue(use.openApi(ImmutableSet.of()) instanceof StringSchema);
        assertEquals(Secret.encrypted(new byte[0]), use.defaultValue());
        assertEquals(Secret.class, use.javaType());
        assertEquals(use, Use.fromJavaType(Secret.class));
        assertEquals(use, Use.fromJavaType(Secret.Encrypted.class));
        assertEquals(use, Use.fromJavaType(Secret.Plaintext.class));
        assertEquals("null", use.toString(null));
        assertEquals("<redacted>", use.toString(Secret.encrypted("AQ==")));
    }

    @Test
    void testUseArray() throws Exception {

        final UseArray<String> use = UseArray.from(UseString.DEFAULT);
        testUse(use, null);
        testUse(use, ImmutableList.of("a", "b", "c"));
        assertTrue(use.openApi(ImmutableSet.of()) instanceof ArraySchema);
        assertEquals(ImmutableList.of(), use.defaultValue());
        final Type javaType = use.javaType();
        assertEquals(List.class, GenericTypeReflector.erase(javaType));
        assertEquals(String.class, GenericTypeReflector.getTypeParameter(javaType, List.class.getTypeParameters()[0]));
        assertEquals(use, Use.fromJavaType(javaType));
        assertEquals("null", use.toString(null));
        assertEquals("[x, y]", use.toString(ImmutableList.of("x", "y")));
    }

    @Test
    void testUseSet() throws Exception {

        final UseSet<String> use = UseSet.from(UseString.DEFAULT);
        testUse(use, null);
        testUse(use, ImmutableSet.of("a", "b", "c"));
        assertTrue(use.openApi(ImmutableSet.of()) instanceof ArraySchema);
        assertEquals(ImmutableSet.of(), use.defaultValue());
        final Type javaType = use.javaType();
        assertEquals(Set.class, GenericTypeReflector.erase(javaType));
        assertEquals(String.class, GenericTypeReflector.getTypeParameter(javaType, Set.class.getTypeParameters()[0]));
        assertEquals(use, Use.fromJavaType(javaType));
        assertEquals("{x, y}", use.toString(ImmutableSet.of("x", "y")));
    }

    @Test
    void testUseMap() throws Exception {

        final UseMap<String> use = UseMap.from(UseString.DEFAULT);
        testUse(use, null);
        testUse(use, ImmutableMap.of("a", "1", "b", "2"));
        assertTrue(use.openApi(ImmutableSet.of()) instanceof MapSchema);
        assertEquals(ImmutableMap.of(), use.defaultValue());
        final Type javaType = use.javaType();
        assertEquals(Map.class, GenericTypeReflector.erase(javaType));
        assertEquals(String.class, GenericTypeReflector.getTypeParameter(javaType, Map.class.getTypeParameters()[1]));
        assertEquals(use, Use.fromJavaType(javaType));
        assertEquals("{x: y}", use.toString(ImmutableMap.of("x", "y")));
    }

    @Test
    void testUseStruct() throws Exception {

        final Namespace namespace = namespace();
        final StructSchema schema = namespace.requireStructSchema("Address");
        final UseInstance use = UseInstance.from(schema, null);

        final Instance init = new Instance(ImmutableMap.of(
                "houseName", "MyHouse",
                "streetName", "MyStreet",
                "country", ReferableSchema.ref("GB"),
                "zip", "12345"
        ));

        testUse(use, null);
        testUse(use, init);

        final Type javaType = use.javaType();
        assertEquals(Map.class, GenericTypeReflector.erase(javaType));
        assertEquals(String.class, GenericTypeReflector.getTypeParameter(javaType, Map.class.getTypeParameters()[0]));
        assertEquals(Object.class, GenericTypeReflector.getTypeParameter(javaType, Map.class.getTypeParameters()[1]));
        assertEquals(String.class, use.javaType(Name.of("houseName")));
        assertEquals("{country: {created: null, hash: null, id: GB, name: null, schema: null, updated: null, version: null}, houseName: MyHouse, streetName: MyStreet, zip: 12345}", use.toString(init));
    }

    @Test
    void testUseRef() throws Exception {

        final Namespace namespace = namespace();
        final InterfaceSchema schema = namespace.requireInterfaceSchema("Pet");
        final UseInstance use = UseInstance.from(schema, null);

        final Instance init = new Instance(ImmutableMap.of(
                "id", "1",
                "schema", "Cat",
                "name", "Pippa"
        ));

        final Instance expect = namespace.requireObjectSchema("Cat").create(init);

        testUse(use, null);
        testUse(use, init, expect);

        final Type javaType = use.javaType();
        assertEquals(Map.class, GenericTypeReflector.erase(javaType));
        assertEquals(String.class, GenericTypeReflector.getTypeParameter(javaType, Map.class.getTypeParameters()[0]));
        assertEquals(Object.class, GenericTypeReflector.getTypeParameter(javaType, Map.class.getTypeParameters()[1]));
        assertEquals(String.class, use.javaType(Name.of(ReferableSchema.ID)));
        assertEquals(Page.class, GenericTypeReflector.erase(use.javaType(Name.of("orders"))));
        assertEquals(Long.class, use.javaType(Name.of("orders", "quantity")));
    }

    @Test
    void testUseView() throws Exception {

        final Namespace namespace = namespace();
        final ViewSchema schema = namespace.requireViewSchema("OrderStats");
        final UseInstance use = UseInstance.from(schema, null);

        final Instance init = new Instance(ImmutableMap.of(
                "store", ReferableSchema.ref("1"),
                "status", "placed",
                "count", 1L,
                "quantity", 10L
        ));

        final Instance expect = schema.create(init);

        testUse(use, null);
        testUse(use, init, expect);
    }

    private <T> void testUse(final Use<T> use, final T init) throws Exception {

        testUse(use, init, init);
    }

    private <T> void testUse(final Use<T> use, final Object init, final T expect) throws Exception {

        final T created = use.create(init, ImmutableSet.of());
        assertTrue(use.areEqual(expect, created), () -> expect + " not equal to " + created);

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
        assertTrue(use.areEqual(expect, deser), () -> expect + " not equal to " + deser);

        final T expanded = use.expand(Name.of(), created, Expander.noop(), ImmutableSet.of());
        assertSame(created, expanded);
    }
}
