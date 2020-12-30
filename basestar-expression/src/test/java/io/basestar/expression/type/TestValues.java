package io.basestar.expression.type;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.util.Pair;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class TestValues {

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
