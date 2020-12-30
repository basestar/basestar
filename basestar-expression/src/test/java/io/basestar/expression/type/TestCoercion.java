package io.basestar.expression.type;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.basestar.expression.exception.TypeConversionException;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;

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
    }

    @Test
    void testToBoolean() {

        assertNull(Coercion.toBoolean(null));
        assertTrue(Coercion.toBoolean(true));
        assertTrue(Coercion.toBoolean("true"));
        assertTrue(Coercion.toBoolean(1L));
        assertFalse(Coercion.toBoolean(false));
        assertFalse(Coercion.toBoolean("false"));
        assertThrows(TypeConversionException.class, () -> Coercion.toBoolean(ImmutableMap.of()));
    }

    @Test
    void testToInteger() {

        assertNull(Coercion.toInteger(null));
        assertEquals(1L, Coercion.toInteger(1.0));
        assertEquals(2L, Coercion.toInteger(2));
        assertEquals(3L, Coercion.toInteger("3"));
        assertEquals(1577836800000L, Coercion.toInteger(LocalDate.parse("2020-01-01")));
        assertEquals(1577836800000L, Coercion.toInteger(Instant.parse("2020-01-01T00:00:00Z")));
        assertThrows(TypeConversionException.class, () -> Coercion.toInteger(ImmutableMap.of()));
        assertThrows(TypeConversionException.class, () -> Coercion.toInteger("invalid"));
    }

    @Test
    void testToNumber() {

        assertNull(Coercion.toFloat(null));
        assertEquals(1.0, Coercion.toFloat(1.0));
        assertEquals(2.0, Coercion.toFloat(2));
        assertEquals(3.0, Coercion.toFloat("3"));
        assertEquals(1577836800000.0, Coercion.toFloat(LocalDate.parse("2020-01-01")));
        assertEquals(1577836800000.0, Coercion.toFloat(Instant.parse("2020-01-01T00:00:00Z")));
        assertThrows(TypeConversionException.class, () -> Coercion.toFloat(ImmutableMap.of()));
        assertThrows(TypeConversionException.class, () -> Coercion.toFloat("invalid"));
    }

    @Test
    void testToString() {

        assertNull(Coercion.toString(null));
        assertEquals("1.0", Coercion.toString(1.0));
        assertEquals("2", Coercion.toString(2));
        assertEquals("3", Coercion.toString("3"));
    }

}
