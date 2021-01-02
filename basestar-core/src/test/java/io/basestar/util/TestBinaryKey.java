package io.basestar.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.*;

class TestBinaryKey {

    @Test
    void testCanCreateKey() {

        final BinaryKey bytes = BinaryKey.from(ImmutableList.of(
                false,
                true,
                456L,
                Instant.parse("2020-01-01T00:00:00Z"),
                LocalDate.parse("2021-01-01"),
                "test string",
                new byte[]{123}
        ));
        assertEquals("AgMEAAAAAAAAAcgHAAABb15m6AAGAAABdrs+cAAFdGVzdCBzdHJpbmcIew==", bytes.toBase64());
        assertThrows(IllegalStateException.class, () -> BinaryKey.from(ImmutableList.of(10D)));
        assertThrows(IllegalStateException.class, () -> BinaryKey.from(ImmutableList.of(ImmutableMap.of("", ""))));
    }

    @Test
    void testSort() {

        final Bytes a = BinaryKey.from(ImmutableList.of(
                "key",
                'c'
        ));
        final Bytes b = BinaryKey.from(ImmutableList.of(
                "keyc"
        ));
        assertTrue(a.compareTo(b) < 0);
    }

    @Test
    void testCannotCorrupt() {

        final Bytes a = BinaryKey.from(ImmutableList.of(
                new byte[]{0},
                new byte[]{1, 2}
        ));
        final Bytes b = BinaryKey.from(ImmutableList.of(
                new byte[]{0, 1, 2}
        ));
        assertTrue(a.compareTo(b) < 0);
    }

    @Test
    void testEscapedBytesOrdering() {

        final Bytes a = BinaryKey.from(ImmutableList.of(
                new byte[]{0, 16, 15, 14}
        ));
        final Bytes b = BinaryKey.from(ImmutableList.of(
                new byte[]{0, 17, 10, 11}
        ));
        assertTrue(a.compareTo(b) < 0);
    }
}
