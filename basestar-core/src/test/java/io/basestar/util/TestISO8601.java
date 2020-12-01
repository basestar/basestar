package io.basestar.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
}
