package io.basestar.util;

import io.basestar.exception.InvalidDateException;
import io.basestar.exception.InvalidDateTimeException;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.Date;

/**
 * Thin shared utility layer to make sure dates and times are always handled consistently.
 *
 * Generally, reducing the precision of a date(time) is supported, but increasing the precision is not, thus any valid
 * datetime will parse as a date, but not necessarily vice-versa.
 */

public class ISO8601 {

    public static final DateTimeFormatter DATE_OUTPUT_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public static final DateTimeFormatter[] DATE_INPUT_FORMATS = {
            DATE_OUTPUT_FORMAT,
            DateTimeFormatter.ISO_LOCAL_DATE,
            DateTimeFormatter.ofPattern("yyyyMMdd"),
            DateTimeFormatter.ofPattern("yyyy-'W'ww-D"),
            DateTimeFormatter.ofPattern("yyyy'W'ww-D"),
            DateTimeFormatter.ofPattern("yyyy-DDD"),
            DateTimeFormatter.ofPattern("yyyyDDD")
    };

    public static final DateTimeFormatter DATE_TIME_OUTPUT_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    public static final DateTimeFormatter[] ZONED_DATE_TIME_INPUT_FORMATS = {
            DateTimeFormatter.ISO_ZONED_DATE_TIME,
            new DateTimeFormatterBuilder().append(DateTimeFormatter.ISO_LOCAL_DATE_TIME).appendPattern("[ ]").appendPattern("z").toFormatter()
    };

    public static final DateTimeFormatter[] LOCAL_DATE_TIME_INPUT_FORMATS = {
            DateTimeFormatter.ISO_LOCAL_DATE_TIME
    };

    public static Instant toDateTime(final TemporalAccessor value) {

        return Instant.from(value);
    }

    public static Instant toDateTime(final Object value) {

        if(value instanceof TemporalAccessor) {
            return toDateTime((TemporalAccessor) value);
        } else if(value instanceof java.sql.Timestamp) {
            return ((java.sql.Timestamp) value).toInstant();
        } else if(value instanceof Date) {
            return ((Date) value).toInstant();
        } else if(value instanceof String) {
            return parseDateTime((String)value);
        } else {
            throw new InvalidDateTimeException(value);
        }
    }

    public static Instant parseDateTime(final String value) {

        for(final DateTimeFormatter formatter: ZONED_DATE_TIME_INPUT_FORMATS) {
            try {
                return formatter.parse(value, ZonedDateTime::from).toInstant();
            } catch (final DateTimeParseException e) {
                // suppress
            }
        }
        for(final DateTimeFormatter formatter: LOCAL_DATE_TIME_INPUT_FORMATS) {
            try {
                return formatter.parse(value, LocalDateTime::from).toInstant(ZoneOffset.UTC);
            } catch (final DateTimeParseException e) {
                // suppress
            }
        }
        return Instant.parse(value);
    }

    public static LocalDate toDate(final TemporalAccessor value) {

        return LocalDate.from(value);
    }

    public static LocalDate toDate(final Object value) {

        if(value instanceof TemporalAccessor) {
            return toDate((TemporalAccessor) value);
        } else if(value instanceof java.sql.Timestamp) {
            return toDate(((java.sql.Timestamp) value).toInstant());
        } else if(value instanceof java.sql.Date) {
            return ((java.sql.Date) value).toLocalDate();
        } else if(value instanceof Date) {
            return toDate(((Date) value).toInstant());
        } else if(value instanceof String) {
            return parseDate((String)value);
        } else {
            throw new InvalidDateException(value);
        }
    }

    public static LocalDate parseDate(final String value) {

        for(final DateTimeFormatter formatter: DATE_INPUT_FORMATS) {
            try {
                return formatter.parse(value, LocalDate::from);
            } catch (final DateTimeParseException e) {
                // suppress
            }
        }
        return parseDateTime(value).atZone(ZoneOffset.UTC).toLocalDate();
    }

    public static String toString(final LocalDate value) {

        return DATE_OUTPUT_FORMAT.format(value);
    }

    public static String toString(final Instant value) {

        return DATE_TIME_OUTPUT_FORMAT.format(LocalDateTime.ofInstant(value, ZoneOffset.UTC));
    }

    public static LocalDate parsePartialDate(final String value) {

        // FIXME: should support non-ambiguous prefix dates, fill in missing values with firsts
        return parseDate(value);
    }

    public static Instant parsePartialDateTime(final String value) {

        // FIXME: should support non-ambiguous prefix datetimes, fill in missing values with zeros/firsts
        return parseDateTime(value);
    }
}
