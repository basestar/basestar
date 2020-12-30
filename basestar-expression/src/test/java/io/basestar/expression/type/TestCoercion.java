package io.basestar.expression.type;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import io.basestar.expression.exception.TypeConversionException;
import io.basestar.secret.Secret;
import io.basestar.util.ISO8601;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class TestCoercion {

    @Test
    void testIsTruthy() {

        assertTrue(Coercion.isTruthy(true));
        assertTrue(Coercion.isTruthy("a"));
        assertTrue(Coercion.isTruthy(1L));
        assertTrue(Coercion.isTruthy(1D));
        assertTrue(Coercion.isTruthy(ImmutableList.of("a")));
        assertTrue(Coercion.isTruthy(ImmutableMap.of("a",1)));
        assertFalse(Coercion.isTruthy(null));
        assertFalse(Coercion.isTruthy(false));
        assertFalse(Coercion.isTruthy(""));
        assertFalse(Coercion.isTruthy("false"));
        assertFalse(Coercion.isTruthy(0L));
        assertFalse(Coercion.isTruthy(0D));
        assertFalse(Coercion.isTruthy(ImmutableList.of()));
        assertFalse(Coercion.isTruthy(ImmutableMap.of()));
        assertThrows(TypeConversionException.class, () -> Coercion.isTruthy(Secret.encrypted(new byte[]{1})));
    }

    @Test
    void testToBoolean() {

        assertNull(Coercion.toBoolean(null));
        assertTrue(Coercion.toBoolean(true));
        assertTrue(Coercion.toBoolean("true"));
        assertTrue(Coercion.toBoolean(1L));
        assertTrue(Coercion.toBoolean(2.0));
        assertFalse(Coercion.toBoolean(false));
        assertFalse(Coercion.toBoolean("false"));
        assertFalse(Coercion.toBoolean(0L));
        assertFalse(Coercion.toBoolean(0.0));
        assertThrows(TypeConversionException.class, () -> Coercion.toBoolean(ImmutableMap.of()));
    }

    @Test
    void testToInteger() {

        assertNull(Coercion.toInteger(null));
        assertEquals(1L, Coercion.toInteger(true));
        assertEquals(0L, Coercion.toInteger(false));
        assertEquals(1L, Coercion.toInteger(1.0));
        assertEquals(2L, Coercion.toInteger(2));
        assertEquals(3L, Coercion.toInteger("3"));
        assertEquals(1577836800000L, Coercion.toInteger(LocalDate.parse("2020-01-01")));
        assertEquals(1577836800000L, Coercion.toInteger(Instant.parse("2020-01-01T00:00:00Z")));
        assertEquals(1577836800000L, Coercion.toInteger(ISO8601.toSqlDate(LocalDate.parse("2020-01-01"))));
        assertEquals(1577836800000L, Coercion.toInteger(ISO8601.toSqlTimestamp(Instant.parse("2020-01-01T00:00:00Z"))));
        assertThrows(TypeConversionException.class, () -> Coercion.toInteger(ImmutableMap.of()));
        assertThrows(TypeConversionException.class, () -> Coercion.toInteger("invalid"));
    }

    @Test
    void testToNumber() {

        assertNull(Coercion.toFloat(null));
        assertEquals(1.0, Coercion.toFloat(true));
        assertEquals(0.0, Coercion.toFloat(false));
        assertEquals(1.0, Coercion.toFloat(1.0));
        assertEquals(2.0, Coercion.toFloat(2));
        assertEquals(3.0, Coercion.toFloat("3"));
        assertEquals(1577836800000.0, Coercion.toFloat(LocalDate.parse("2020-01-01")));
        assertEquals(1577836800000.0, Coercion.toFloat(Instant.parse("2020-01-01T00:00:00Z")));
        assertEquals(1577836800000.0, Coercion.toFloat(ISO8601.toSqlDate(LocalDate.parse("2020-01-01"))));
        assertEquals(1577836800000.0, Coercion.toFloat(ISO8601.toSqlTimestamp(Instant.parse("2020-01-01T00:00:00Z"))));
        assertThrows(TypeConversionException.class, () -> Coercion.toFloat(ImmutableMap.of()));
        assertThrows(TypeConversionException.class, () -> Coercion.toFloat("invalid"));
    }

    @Test
    void testToString() {

        assertNull(Coercion.toString(null));
        assertEquals("1.0", Coercion.toString(1.0));
        assertEquals("2", Coercion.toString(2));
        assertEquals("3", Coercion.toString("3"));
        assertEquals("2020-01-01", Coercion.toString(LocalDate.parse("2020-01-01")));
        assertEquals("2020-01-01T00:00:00.000Z", Coercion.toString(Instant.parse("2020-01-01T00:00:00Z")));
        assertEquals("2020-01-01", Coercion.toString(ISO8601.toSqlDate(LocalDate.parse("2020-01-01"))));
        assertEquals("2020-01-01T00:00:00.000Z", Coercion.toString(ISO8601.toSqlTimestamp(Instant.parse("2020-01-01T00:00:00Z"))));
        assertEquals("AQ==", Coercion.toString(new byte[]{1}));
        assertEquals("{}", Coercion.toString(ImmutableMap.of()));
    }

    @Test
    void testToDate() {

        assertNull(Coercion.toDate(null));
        assertEquals(LocalDate.parse("2020-01-01"), Coercion.toDate("2020-01-01"));
    }

    @Test
    void testToDateTime() {

        assertNull(Coercion.toDateTime(null));
        assertEquals(Instant.parse("2020-01-01T00:00:00.000Z"), Coercion.toDateTime("2020-01-01T00:00:00.000Z"));
    }

    @Test
    void testToSet() {

        assertNull(Coercion.toSet(null, v -> v));
        assertEquals(ImmutableSet.of("a", "b", "c"), Coercion.toSet(ImmutableList.of("c", "b", "a"), v -> v));
        assertTrue(Coercion.toSet(ImmutableList.of("c", "b", "a"), HashSet.class, v -> v) instanceof HashSet);
        assertTrue(Coercion.toSet(ImmutableList.of("c", "b", "a"), TreeSet.class, v -> v) instanceof TreeSet);
        assertTrue(Coercion.toSet(ImmutableList.of("c", "b", "a"), SortedSet.class, v -> v) instanceof SortedSet);
        assertTrue(Coercion.toSet(ImmutableList.of("c", "b", "a"), NavigableSet.class, v -> v) instanceof NavigableSet);
        assertThrows(TypeConversionException.class, () -> Coercion.toSet(ImmutableMap.of(), v -> v));
    }

    @Test
    void testToList() {

        assertNull(Coercion.toSet(null, v -> v));
        assertEquals(ImmutableList.of("a", "b", "c"), Coercion.toList(ImmutableSortedSet.of("a", "b", "c"), v -> v));
        assertTrue(Coercion.toList(ImmutableList.of("c", "b", "a"), ArrayList.class, v -> v) instanceof ArrayList);
        assertTrue(Coercion.toList(ImmutableList.of("c", "b", "a"), LinkedList.class, v -> v) instanceof LinkedList);
        assertThrows(TypeConversionException.class, () -> Coercion.toList(ImmutableMap.of(), v -> v));
    }

    @Test
    void testToMap() {

        assertNull(Coercion.toMap(null, k -> k, v -> v));
        assertEquals(ImmutableMap.of("a", "B"), Coercion.toMap(ImmutableMap.of("a", "b"), Object::toString, v -> v.toString().toUpperCase()));
        assertTrue(Coercion.toMap(ImmutableMap.of("a", "b"), HashMap.class, k -> k, v -> v) instanceof HashMap);
        assertTrue(Coercion.toMap(ImmutableMap.of("a", "b"), TreeMap.class, k -> k, v -> v) instanceof TreeMap);
        assertTrue(Coercion.toMap(ImmutableMap.of("a", "b"), SortedMap.class, k -> k, v -> v) instanceof SortedMap);
        assertTrue(Coercion.toMap(ImmutableMap.of("a", "b"), NavigableMap.class, k -> k, v -> v) instanceof NavigableMap);
        assertThrows(TypeConversionException.class, () -> Coercion.toMap(ImmutableList.of(), k -> k, v -> v));
    }

    @Test
    void testToBinary() {

        assertNull(Coercion.toBinary(null));
        assertArrayEquals(new byte[]{1}, Coercion.toBinary("AQ=="));
        assertArrayEquals(new byte[]{1}, Coercion.toBinary(new byte[]{1}));
        assertArrayEquals(new byte[]{1}, Coercion.toBinary(ByteBuffer.wrap(new byte[]{1})));
        assertThrows(TypeConversionException.class, () -> Coercion.toBinary(ImmutableList.of()));
    }
}
