package io.basestar.schema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.expression.type.exception.TypeConversionException;
import io.basestar.schema.exception.UnexpectedTypeException;
import io.basestar.schema.use.*;
import io.basestar.secret.Secret;
import io.basestar.util.ISO8601;
import org.junit.jupiter.api.Test;

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
        assertNull(suppressing.createArray(type, Secret.encrypted(new byte[0]), ImmutableSet.of()));
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
        assertNull(suppressing.createDateTime(UseDateTime.DEFAULT, Secret.encrypted(new byte[0]), ImmutableSet.of()));
    }
}
