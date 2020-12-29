package io.basestar.expression.type;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.expression.type.exception.TypeConversionException;
import io.basestar.util.Pair;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class TestValues {

    @Test
    void testIsTruthy() {

        assertTrue(Values.isTruthy(true));
        assertTrue(Values.isTruthy("a"));
        assertTrue(Values.isTruthy(1L));
        assertTrue(Values.isTruthy(1D));
        assertTrue(Values.isTruthy(ImmutableList.of("a")));
        assertTrue(Values.isTruthy(ImmutableMap.of("a",1)));
        assertFalse(Values.isTruthy(null));
        assertFalse(Values.isTruthy(false));
        assertFalse(Values.isTruthy(""));
        assertFalse(Values.isTruthy("false"));
        assertFalse(Values.isTruthy(0L));
        assertFalse(Values.isTruthy(0D));
        assertFalse(Values.isTruthy(ImmutableList.of()));
        assertFalse(Values.isTruthy(ImmutableMap.of()));
    }

    @Test
    void testToBoolean() {

        assertNull(Values.toBoolean(null));
        assertTrue(Values.toBoolean(true));
        assertTrue(Values.toBoolean("true"));
        assertTrue(Values.toBoolean(1L));
        assertFalse(Values.toBoolean(false));
        assertFalse(Values.toBoolean("false"));
        assertThrows(TypeConversionException.class, () -> Values.toBoolean(ImmutableMap.of()));
    }

    @Test
    void testToInteger() {

        assertNull(Values.toInteger(null));
        assertEquals(1L, Values.toInteger(1.0));
        assertEquals(2L, Values.toInteger(2));
        assertEquals(3L, Values.toInteger("3"));
        assertEquals(1577836800000L, Values.toInteger(LocalDate.parse("2020-01-01")));
        assertEquals(1577836800000L, Values.toInteger(Instant.parse("2020-01-01T00:00:00Z")));
        assertThrows(TypeConversionException.class, () -> Values.toInteger(ImmutableMap.of()));
        assertThrows(TypeConversionException.class, () -> Values.toInteger("invalid"));
    }

    @Test
    void testToNumber() {

        assertNull(Values.toFloat(null));
        assertEquals(1.0, Values.toFloat(1.0));
        assertEquals(2.0, Values.toFloat(2));
        assertEquals(3.0, Values.toFloat("3"));
        assertEquals(1577836800000.0, Values.toFloat(LocalDate.parse("2020-01-01")));
        assertEquals(1577836800000.0, Values.toFloat(Instant.parse("2020-01-01T00:00:00Z")));
        assertThrows(TypeConversionException.class, () -> Values.toFloat(ImmutableMap.of()));
        assertThrows(TypeConversionException.class, () -> Values.toFloat("invalid"));
    }

    @Test
    void testToString() {

        assertNull(Values.toString(null));
        assertEquals("1.0", Values.toString(1.0));
        assertEquals("2", Values.toString(2));
        assertEquals("3", Values.toString("3"));
    }

    @Test
    void testIsInteger() {

        assertTrue(Values.isInteger(1));
        assertTrue(Values.isInteger(1L));
        assertFalse(Values.isInteger(1.0));
    }

    @Test
    void testIsFloat() {

        assertFalse(Values.isFloat(1));
        assertFalse(Values.isFloat(1L));
        assertTrue(Values.isFloat(1.0));
    }

    @Test
    void testPromote() {

        assertEquals(Pair.of("a", "b"), Values.promote("a", "b"));
        assertEquals(Pair.of(3.0, 4.0), Values.promote(3, 4.0));
        assertEquals(Pair.of(2L, 0L), Values.promote(2, false));
    }

    @Test
    void testEquals() {

        assertTrue(Values.equals("1", "1"));
        assertTrue(Values.equals(1.0, 1L));
        assertFalse(Values.equals(2.0, 1L));
        assertTrue(Values.equals(ImmutableList.of(1.0, 2L), ImmutableList.of(1L, 2.0)));
        assertFalse(Values.equals(ImmutableList.of(1.0, 2L), ImmutableList.of(1L, 2.5)));
        assertTrue(Values.equals(ImmutableMap.of("a", 1.0, "b", 2L), ImmutableMap.of("a", 1L, "b", 2.0)));
        assertFalse(Values.equals(ImmutableMap.of("a", 1.0, "b", 2L), ImmutableMap.of("a", 1L, "b", 2.5)));
    }

    @Test
    void testDefaultValue() {

        assertEquals(false, Values.defaultValue(boolean.class));
        assertEquals(false, Values.defaultValue(Boolean.class));
        assertEquals(0, Values.defaultValue(Integer.class));
        assertEquals(0L, Values.defaultValue(long.class));
        assertEquals("", Values.defaultValue(String.class));
        assertEquals(ImmutableList.of(), Values.defaultValue(List.class));
        assertEquals(ImmutableSet.of(), Values.defaultValue(Set.class));
        assertEquals(ImmutableMap.of(), Values.defaultValue(Map.class));
    }
}
