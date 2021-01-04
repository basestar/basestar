package io.basestar.expression.type;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestNumbers {

    @Test
    void testIsInteger() {

        assertTrue(Numbers.isInteger(1));
        assertTrue(Numbers.isInteger(1L));
        assertFalse(Numbers.isInteger(1.0));
    }

    @Test
    void testIsFloat() {

        assertFalse(Numbers.isFloat(1));
        assertFalse(Numbers.isFloat(1L));
        assertTrue(Numbers.isFloat(1.0));
    }
}
