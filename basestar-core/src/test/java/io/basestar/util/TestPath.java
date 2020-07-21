package io.basestar.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestPath {

    @Test
    public void testCanonical() {

        assertEquals(Path.parse("abc/xyz/Type"), Path.parse("abc").with(Path.parse("xyz/Type")).canonical());
        assertEquals(Path.parse("xyz/abc/Type"), Path.parse("abc/xyz").with(Path.parse("../../xyz/abc/Type")).canonical());
        assertEquals(Path.parse("../src/Type"), Path.parse("../src").with(Path.parse("Type")).canonical());
    }
}
