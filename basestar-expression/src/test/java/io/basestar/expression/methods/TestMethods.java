package io.basestar.expression.methods;

import io.basestar.util.Name;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Type;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class TestMethods {

    @Test
    void testVariadic() {

        final Methods methods = Methods.builder().defaults().build();
        assertNotNull(methods.callable(Name.of("concat"), new Type[] { String.class, String.class, String.class}));
    }
}
