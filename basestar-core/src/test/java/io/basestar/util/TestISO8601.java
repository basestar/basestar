package io.basestar.util;

import com.google.common.collect.ImmutableMap;
import io.basestar.exception.InvalidDateTimeException;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

class TestISO8601 {

    @Test
    void testParseDateTime() {

        // FIXME: is this rounding expected?
        assertEquals("2020-07-29T11:21:38.501Z", ISO8601.toString(ISO8601.parseDateTime("2020-07-29T11:21:38.501736Z")));
        assertEquals("2020-01-01T00:00:00.000Z", ISO8601.toString(ISO8601.parseDateTime("2020-01-01T00:00:00.000Z")));
        assertEquals("2020-01-01T00:00:00.000Z", ISO8601.toString(ISO8601.parseDateTime("2020-01-01T00:00:00.000")));
        assertEquals("2020-01-01T00:00:00.000Z", ISO8601.toString(ISO8601.parseDateTime("2020-01-01T00:00:00")));
        assertEquals("2020-01-01T00:00:00.000Z", ISO8601.toString(ISO8601.parseDateTime("2020-01-01T00:00")));
        assertEquals("2020-01-01T01:00:00.000Z", ISO8601.toString(ISO8601.parseDateTime("2020-01-01T00:00:00-01:00")));
        assertEquals("2020-01-01T00:00:00.000Z", ISO8601.toString(ISO8601.parseDateTime("2020-01-01T00:00:00 GMT")));
        assertEquals("2020-01-01T00:00:00.000Z", ISO8601.toString(ISO8601.parseDateTime("2020-01-01T02:00:00 EEST")));
        assertEquals("2020-01-01T00:00:00.000Z", ISO8601.toString(ISO8601.parseDateTime("2020-01-01T00:00:00 Europe/London")));
        assertEquals("2020-01-01T00:00:00.000Z", ISO8601.toString(ISO8601.parseDateTime("2020-01-01T00:00:00GMT")));
        assertEquals("2020-01-01T00:00:00.000Z", ISO8601.toString(ISO8601.parseDateTime("2020-01-01T01:00:00GMT+01:00")));
        assertEquals("2020-08-12T14:34:58.977Z", ISO8601.toString(ISO8601.parseDateTime("1597242898977")));
    }

    @Test
    void testParseDate() {

        assertEquals("2019-09-24", ISO8601.toString(ISO8601.parseDate("20190924")));
        assertEquals("2020-01-01", ISO8601.toString(ISO8601.parseDate("20200101")));
        assertEquals("2020-01-01", ISO8601.toString(ISO8601.parseDate("2020-01-01")));
        assertEquals("2020-01-01", ISO8601.toString(ISO8601.parseDate("2020-W01-1")));
        assertEquals("2020-01-01", ISO8601.toString(ISO8601.parseDate("2020W01-1")));
        assertEquals("2020-01-01", ISO8601.toString(ISO8601.parseDate("2020-001")));
        assertEquals("2020-01-01", ISO8601.toString(ISO8601.parseDate("2020001")));
        assertEquals("2020-01-01", ISO8601.toString(ISO8601.parseDate("2020-01-01T00:00:00-01:00")));
    }

    @Test
    void testToDate() {

        assertNull(ISO8601.toDate((Object)null));
        assertEquals(LocalDate.parse("2020-01-01"), ISO8601.toDate("2020-01-01"));
        assertEquals(LocalDate.parse("2020-01-01"), ISO8601.toDate("2020-01-01T00:00:00.0Z"));
        assertEquals(LocalDate.parse("2020-01-01"), ISO8601.toDate((Object)Instant.parse("2020-01-01T00:00:00.000Z")));
        assertEquals(LocalDate.parse("1970-01-01"), ISO8601.toDate(0L));
        assertEquals(LocalDate.parse("1970-01-01"), ISO8601.toDate(new Date(0L)));
        assertEquals(LocalDate.parse("1970-01-01"), ISO8601.toDate(new java.sql.Date(0L)));
        assertEquals(LocalDate.parse("1970-01-01"), ISO8601.toDate(new java.sql.Timestamp(0L)));
        assertThrows(InvalidDateTimeException.class, () -> ISO8601.toDate(ImmutableMap.of()));
    }

    @Test
    void testToDateTime() {

        assertNull(ISO8601.toDateTime((Object)null));
        assertEquals(Instant.parse("2020-01-01T00:00:00.0Z"), ISO8601.toDateTime("2020-01-01"));
        assertEquals(Instant.parse("2020-01-01T00:00:00.0Z"), ISO8601.toDateTime("2020-01-01T00:00:00.000Z"));
        assertEquals(Instant.parse("2020-01-01T00:00:00.0Z"), ISO8601.toDateTime((Object)LocalDate.parse("2020-01-01")));
        assertEquals(Instant.parse("1970-01-01T00:00:00.0Z"), ISO8601.toDateTime(0L));
        assertEquals(Instant.parse("1970-01-01T00:00:00.0Z"), ISO8601.toDateTime(new Date(0L)));
        assertEquals(Instant.parse("1970-01-01T00:00:00.0Z"), ISO8601.toDateTime(new java.sql.Date(0L)));
        assertEquals(Instant.parse("1970-01-01T00:00:00.0Z"), ISO8601.toDateTime(new java.sql.Timestamp(0L)));
        assertThrows(InvalidDateTimeException.class, () -> ISO8601.toDateTime(ImmutableMap.of()));
    }
}
