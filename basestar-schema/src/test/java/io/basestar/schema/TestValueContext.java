package io.basestar.schema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.expression.exception.TypeConversionException;
import io.basestar.schema.exception.UnexpectedTypeException;
import io.basestar.schema.use.*;
import io.basestar.schema.util.ValueContext;
import io.basestar.secret.Secret;
import io.basestar.util.Bytes;
import io.basestar.util.ISO8601;
import io.basestar.util.Page;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TestValueContext {

    @Test
    void testCreateBoolean() {

        final ValueContext standard = ValueContext.standard();
        assertEquals(true, standard.createBoolean(UseBoolean.DEFAULT, "test", ImmutableSet.of()));
        assertEquals(true, standard.createBoolean(UseBoolean.DEFAULT, true, ImmutableSet.of()));
        assertEquals(true, standard.createBoolean(UseBoolean.DEFAULT, 3L, ImmutableSet.of()));
        assertEquals(false, standard.createBoolean(UseBoolean.DEFAULT, 0, ImmutableSet.of()));
        assertEquals(false, standard.createBoolean(UseBoolean.DEFAULT, "", ImmutableSet.of()));

        assertThrows(TypeConversionException.class, () -> {
            standard.createBoolean(UseBoolean.DEFAULT, Secret.encrypted(new byte[0]), ImmutableSet.of());
        });

        final ValueContext suppressing = ValueContext.suppressing();
        assertNull(suppressing.createBoolean(UseBoolean.DEFAULT, null, ImmutableSet.of()));
        assertNull(suppressing.createBoolean(UseBoolean.DEFAULT, Secret.encrypted(new byte[0]), ImmutableSet.of()));
    }

    @Test
    void testCreateInteger() {

        final ValueContext standard = ValueContext.standard();
        assertEquals(3L, standard.createInteger(UseInteger.DEFAULT, 3L, ImmutableSet.of()));
        assertEquals(4L, standard.createInteger(UseInteger.DEFAULT, "4", ImmutableSet.of()));
        assertEquals(5L, standard.createInteger(UseInteger.DEFAULT, 5.0, ImmutableSet.of()));

        assertThrows(TypeConversionException.class, () -> {
            standard.createInteger(UseInteger.DEFAULT, Secret.encrypted(new byte[0]), ImmutableSet.of());
        });

        final ValueContext suppressing = ValueContext.suppressing();
        assertNull(suppressing.createInteger(UseInteger.DEFAULT, null, ImmutableSet.of()));
        assertNull(suppressing.createInteger(UseInteger.DEFAULT, Secret.encrypted(new byte[0]), ImmutableSet.of()));
    }

    @Test
    void testCreateNumber() {

        final ValueContext standard = ValueContext.standard();
        assertEquals(3.0, standard.createNumber(UseNumber.DEFAULT, 3L, ImmutableSet.of()));
        assertEquals(4.0, standard.createNumber(UseNumber.DEFAULT, "4", ImmutableSet.of()));
        assertEquals(5.0, standard.createNumber(UseNumber.DEFAULT, 5.0, ImmutableSet.of()));

        assertThrows(TypeConversionException.class, () -> {
            standard.createNumber(UseNumber.DEFAULT, Secret.encrypted(new byte[0]), ImmutableSet.of());
        });

        final ValueContext suppressing = ValueContext.suppressing();
        assertNull(suppressing.createNumber(UseNumber.DEFAULT, null, ImmutableSet.of()));
        assertNull(suppressing.createNumber(UseNumber.DEFAULT, Secret.encrypted(new byte[0]), ImmutableSet.of()));
    }

    @Test
    void testCreateString() {

        final ValueContext standard = ValueContext.standard();
        assertEquals("test", standard.createString(UseString.DEFAULT, "test", ImmutableSet.of()));
        assertEquals("true", standard.createString(UseString.DEFAULT, true, ImmutableSet.of()));
        assertEquals("3", standard.createString(UseString.DEFAULT, 3L, ImmutableSet.of()));

        assertThrows(TypeConversionException.class, () -> {
            standard.createString(UseString.DEFAULT, Secret.encrypted(new byte[0]), ImmutableSet.of());
        });

        final ValueContext suppressing = ValueContext.suppressing();
        assertNull(suppressing.createString(UseString.DEFAULT, null, ImmutableSet.of()));
        assertNull(suppressing.createString(UseString.DEFAULT, Secret.encrypted(new byte[0]), ImmutableSet.of()));
    }

    @Test
    void testCreateArray() {

        final UseArray<String> type = new UseArray<>(UseString.DEFAULT);

        final ValueContext standard = ValueContext.standard();
        assertEquals(ImmutableList.of("test"), standard.createArray(type, ImmutableList.of("test"), ImmutableSet.of()));

        assertThrows(UnexpectedTypeException.class, () -> {
            standard.createArray(type, Secret.encrypted(new byte[0]), ImmutableSet.of());
        });

        final ValueContext suppressing = ValueContext.suppressing();
        assertNull(suppressing.createArray(type, null, ImmutableSet.of()));
        assertNull(suppressing.createArray(type, Secret.encrypted(new byte[0]), ImmutableSet.of()));
    }

    @Test
    void testCreatePage() {

        final UsePage<String> type = new UsePage<>(UseString.DEFAULT);

        final ValueContext standard = ValueContext.standard();
        assertEquals(Page.from(ImmutableList.of("test")), standard.createPage(type, ImmutableList.of("test"), ImmutableSet.of()));

        final Page.Token token = new Page.Token(new byte[]{1});
        final Page.Stats stats = Page.Stats.fromTotal(10);
        assertEquals(new Page<>(ImmutableList.of("1"), token, stats), standard.createPage(type, new Page<>(ImmutableList.of(1), token, stats), ImmutableSet.of()));

        assertThrows(UnexpectedTypeException.class, () -> {
            standard.createPage(type, Secret.encrypted(new byte[0]), ImmutableSet.of());
        });

        final ValueContext suppressing = ValueContext.suppressing();
        assertNull(suppressing.createPage(type, null, ImmutableSet.of()));
        assertNull(suppressing.createPage(type, Secret.encrypted(new byte[0]), ImmutableSet.of()));
    }

    @Test
    void testCreateSet() {

        final UseSet<String> type = new UseSet<>(UseString.DEFAULT);

        final ValueContext standard = ValueContext.standard();
        assertEquals(ImmutableSet.of("test"), standard.createSet(type, ImmutableSet.of("test"), ImmutableSet.of()));

        assertThrows(UnexpectedTypeException.class, () -> {
            standard.createSet(type, Secret.encrypted(new byte[0]), ImmutableSet.of());
        });

        final ValueContext suppressing = ValueContext.suppressing();
        assertNull(suppressing.createSet(type, null, ImmutableSet.of()));
        assertNull(suppressing.createSet(type, Secret.encrypted(new byte[0]), ImmutableSet.of()));
    }

    @Test
    void testCreateMap() {

        final UseMap<String> type = new UseMap<>(UseString.DEFAULT);

        final ValueContext standard = ValueContext.standard();
        assertEquals(ImmutableMap.of("key", "test"), standard.createMap(type, ImmutableMap.of("key", "test"), ImmutableSet.of()));

        assertThrows(UnexpectedTypeException.class, () -> {
            standard.createMap(type, Secret.encrypted(new byte[0]), ImmutableSet.of());
        });

        final ValueContext suppressing = ValueContext.suppressing();
        assertNull(suppressing.createMap(type, null, ImmutableSet.of()));
        assertNull(suppressing.createMap(type, Secret.encrypted(new byte[0]), ImmutableSet.of()));
    }

    @Test
    void testCreateDate() {

        final ValueContext standard = ValueContext.standard();
        assertEquals(ISO8601.parseDate("2020-01-01"), standard.createDate(UseDate.DEFAULT, "2020-01-01", ImmutableSet.of()));

        assertThrows(TypeConversionException.class, () -> {
            standard.createDate(UseDate.DEFAULT, Secret.encrypted(new byte[0]), ImmutableSet.of());
        });

        final ValueContext suppressing = ValueContext.suppressing();
        assertNull(suppressing.createDate(UseDate.DEFAULT, null, ImmutableSet.of()));
        assertNull(suppressing.createDate(UseDate.DEFAULT, Secret.encrypted(new byte[0]), ImmutableSet.of()));
    }

    @Test
    void testCreateDateTime() {

        final ValueContext standard = ValueContext.standard();
        assertEquals(ISO8601.parseDateTime("2020-01-01T01:02:03.456Z"), standard.createDateTime(UseDateTime.DEFAULT, "2020-01-01T01:02:03.456Z", ImmutableSet.of()));

        assertThrows(TypeConversionException.class, () -> {
            standard.createDateTime(UseDateTime.DEFAULT, Secret.encrypted(new byte[0]), ImmutableSet.of());
        });

        final ValueContext suppressing = ValueContext.suppressing();
        assertNull(suppressing.createDateTime(UseDateTime.DEFAULT, null, ImmutableSet.of()));
        assertNull(suppressing.createDateTime(UseDateTime.DEFAULT, Secret.encrypted(new byte[0]), ImmutableSet.of()));
    }

    @Test
    void testCreateSecret() {

        final ValueContext standard = ValueContext.standard();
        assertEquals(Secret.encrypted(new byte[]{1, 2, 3}), standard.createSecret(UseSecret.DEFAULT, Secret.encrypted(new byte[]{1, 2, 3}), ImmutableSet.of()));
        assertEquals(Secret.encrypted(new byte[]{1, 2, 3}), standard.createSecret(UseSecret.DEFAULT, new byte[]{1, 2, 3}, ImmutableSet.of()));

        assertThrows(TypeConversionException.class, () -> {
            standard.createSecret(UseSecret.DEFAULT, "x", ImmutableSet.of());
        });

        final ValueContext suppressing = ValueContext.suppressing();
        assertNull(suppressing.createSecret(UseSecret.DEFAULT,null, ImmutableSet.of()));
        assertNull(suppressing.createSecret(UseSecret.DEFAULT,"x", ImmutableSet.of()));
    }

    @Test
    void testCreateEnum() throws IOException {

        final Namespace namespace = Namespace.load(TestInterfaceSchema.class.getResource("/schema/Petstore.yml"));

        final UseEnum use = UseEnum.from(namespace.requireEnumSchema("PetStatus"), null);

        final ValueContext standard = ValueContext.standard();
        assertEquals("available", standard.createEnum(use, "available", ImmutableSet.of()));

        assertThrows(UnexpectedTypeException.class, () -> {
            standard.createEnum(use, "x", ImmutableSet.of());
        });

        final ValueContext suppressing = ValueContext.suppressing();
        assertNull(suppressing.createEnum(use,null, ImmutableSet.of()));
        assertNull(suppressing.createEnum(use,"x", ImmutableSet.of()));
    }

    @Test
    void testCreateRef() throws IOException {

        final Namespace namespace = Namespace.load(TestInterfaceSchema.class.getResource("/schema/Petstore.yml"));

        final UseRef use = UseRef.from(namespace.requireInterfaceSchema("Pet"), null);

        final ValueContext standard = ValueContext.standard();
        assertEquals(ReferableSchema.ref("1"), standard.createRef(use, ImmutableMap.of("id", "1"), ImmutableSet.of()));
        final Map<String, Object> expanded = standard.createRef(use, ImmutableMap.of("id", "1", "schema", "Cat"), ImmutableSet.of());
        assertEquals("1", Instance.getId(expanded));
        assertTrue(expanded.containsKey("catBreed"));
        assertTrue(expanded.containsKey("hash"));

        assertThrows(UnexpectedTypeException.class, () -> {
            standard.createRef(use, "x", ImmutableSet.of());
        });

        final ValueContext suppressing = ValueContext.suppressing();
        assertNull(suppressing.createRef(use,null, ImmutableSet.of()));
        assertNull(suppressing.createRef(use,"x", ImmutableSet.of()));
    }

    @Test
    void testCreateVersionedRef() throws IOException {

        final Namespace namespace = Namespace.load(TestInterfaceSchema.class.getResource("/schema/Petstore.yml"));

        final UseRef use = UseRef.from(namespace.requireInterfaceSchema("Pet"), ImmutableMap.of("versioned", true));

        final ValueContext standard = ValueContext.standard();
        assertEquals(ReferableSchema.versionedRef("1", 2L), standard.createRef(use, ImmutableMap.of("id", "1", "version", "2"), ImmutableSet.of()));
        final Map<String, Object> expanded = standard.createRef(use, ImmutableMap.of("id", "1", "version", 2, "schema", "Cat"), ImmutableSet.of());
        assertEquals("1", Instance.getId(expanded));
        assertEquals(2L, Instance.getVersion(expanded));
        assertTrue(expanded.containsKey("catBreed"));
        assertTrue(expanded.containsKey("hash"));

        assertThrows(UnexpectedTypeException.class, () -> {
            standard.createRef(use, ReferableSchema.ref("1"), ImmutableSet.of());
        });

        final ValueContext suppressing = ValueContext.suppressing();
        assertNull(suppressing.createRef(use,null, ImmutableSet.of()));
        assertNull(suppressing.createRef(use,ReferableSchema.ref("1"), ImmutableSet.of()));
    }

    @Test
    void testCreateView() throws IOException {

        final Namespace namespace = Namespace.load(TestInterfaceSchema.class.getResource("/schema/Petstore.yml"));

        final UseView use = UseView.from(namespace.requireViewSchema("PetStats"), null);

        final ValueContext standard = ValueContext.standard();
        final Map<String, Object> record = standard.createView(use, ImmutableMap.of("schema", "Cat", "count", 10), ImmutableSet.of());
        assertEquals(Bytes.valueOf(UseBinary.binaryKey(ImmutableList.of("Cat"))), record.get("__key"));
        assertEquals("Cat", record.get("schema"));
        assertEquals(10L, record.get("count"));

        assertThrows(UnexpectedTypeException.class, () -> {
            standard.createView(use, "x", ImmutableSet.of());
        });

        final ValueContext suppressing = ValueContext.suppressing();
        assertNull(suppressing.createView(use,null, ImmutableSet.of()));
        assertNull(suppressing.createView(use,"x", ImmutableSet.of()));
    }

    @Test
    void testCreateBinary() {

        final ValueContext standard = ValueContext.standard();
        assertEquals(Bytes.valueOf(1), standard.createBinary(UseBinary.DEFAULT, "AQ", ImmutableSet.of()));
        assertEquals(Bytes.valueOf(1), standard.createBinary(UseBinary.DEFAULT, new byte[]{1}, ImmutableSet.of()));

        assertThrows(TypeConversionException.class, () -> {
            standard.createBinary(UseBinary.DEFAULT, Secret.encrypted(new byte[0]), ImmutableSet.of());
        });

        final ValueContext suppressing = ValueContext.suppressing();
        assertNull(suppressing.createBinary(UseBinary.DEFAULT, null, ImmutableSet.of()));
        assertNull(suppressing.createBinary(UseBinary.DEFAULT, Secret.encrypted(new byte[0]), ImmutableSet.of()));
    }
}
