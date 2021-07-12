package io.basestar.util;

import io.basestar.exception.InvalidDateTimeException;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.util.Date;

/**
 * Thin shared utility layer to make sure dates and times are always handled consistently.
 *
 * Generally, reducing the precision of a date(time) is supported, but increasing the precision is not, thus any valid
 * datetime will parse as a date, but not necessarily vice-versa.
 */

public class ISO8601 {

    private static final DateTimeFormatter DATE_OUTPUT_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private ISO8601() {

    }

    private static final DateTimeFormatter[] DATE_INPUT_FORMATS = {
            DATE_OUTPUT_FORMAT,
            DateTimeFormatter.ISO_LOCAL_DATE,
            DateTimeFormatter.ofPattern("yyyyMMdd"),
            DateTimeFormatter.ofPattern("yyyy-'W'ww-D"),
            DateTimeFormatter.ofPattern("yyyy'W'ww-D"),
            DateTimeFormatter.ofPattern("yyyy-DDD"),
            DateTimeFormatter.ofPattern("yyyyDDD")
    };

    private static final DateTimeFormatter DATE_TIME_OUTPUT_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    private static final DateTimeFormatter[] ZONED_DATE_TIME_INPUT_FORMATS = {
            DateTimeFormatter.ISO_ZONED_DATE_TIME,
            new DateTimeFormatterBuilder().append(DateTimeFormatter.ISO_LOCAL_DATE_TIME).appendPattern("[ ]").appendPattern("z").toFormatter()
    };

    private static final DateTimeFormatter[] LOCAL_DATE_TIME_INPUT_FORMATS = {
            DateTimeFormatter.ISO_LOCAL_DATE_TIME
    };

    public static Instant toDateTime(final TemporalAccessor value) {

        if(value instanceof LocalDate) {
            return ((LocalDate) value).atStartOfDay().toInstant(ZoneOffset.UTC);
        } else {
            return Instant.from(value);
        }
    }

    public static Instant toDateTime(final Object value) {

        if(value == null) {
            return null;
        } else if(value instanceof TemporalAccessor) {
            return toDateTime((TemporalAccessor) value);
        } else if(value instanceof java.sql.Timestamp) {
            return ((java.sql.Timestamp) value).toInstant();
        } else if(value instanceof java.sql.Date) {
            return toDateTime(((java.sql.Date) value).toLocalDate());
        } else if(value instanceof Date) {
            return ((Date) value).toInstant();
        } else if(value instanceof String) {
            return parseDateTime((String)value);
        } else if(value instanceof Number) {
            return toDateTime(((Number)value).longValue());
        } else {
            throw new InvalidDateTimeException(value);
        }
    }

    private static Instant toDateTime(final long value) {

        return Instant.ofEpochMilli(value);
    }

    public static Instant parseDateTime(final String value) {

        if(Text.isInteger(value)) {
            return toDateTime(Long.parseLong(value));
        }
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
        for(final DateTimeFormatter formatter: DATE_INPUT_FORMATS) {
            try {
                return toDateTime(formatter.parse(value, LocalDate::from));
            } catch (final DateTimeParseException e) {
                // suppress
            }
        }
        try {
            return Instant.parse(value);
        } catch (final DateTimeParseException e) {
            throw new InvalidDateTimeException(value);
        }
    }

    public static Instant parseDateTime(final String value, final String format) {

        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
        try {
            return formatter.parse(value, Instant::from);
        } catch (final DateTimeParseException e) {
            // suppress
        }
        try {
            return formatter.parse(value, LocalDateTime::from).toInstant(ZoneOffset.UTC);
        } catch (final DateTimeParseException e) {
            throw new InvalidDateTimeException(value);
        }
    }

    public static LocalDate toDate(final TemporalAccessor value) {

        if(value instanceof Instant) {
            return ((Instant) value).atZone(ZoneOffset.UTC).toLocalDate();
        } else {
            return LocalDate.from(value);
        }
    }

    public static LocalDate toDate(final Object value) {

        if(value == null) {
            return null;
        } else if(value instanceof TemporalAccessor) {
            return toDate((TemporalAccessor) value);
        } else if(value instanceof java.sql.Timestamp) {
            return toDate(((java.sql.Timestamp) value).toInstant());
        } else if(value instanceof java.sql.Date) {
            return ((java.sql.Date) value).toLocalDate();
        } else if(value instanceof Date) {
            return toDate(((Date) value).toInstant());
        } else if(value instanceof String) {
            return parseDate((String) value);
        } else if(value instanceof Number) {
            return toDate(toDateTime(((Number)value).longValue()));
        } else {
            throw new InvalidDateTimeException(value);
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

    public static LocalDate parseDate(final String value, final String format) {

        try {
            return DateTimeFormatter.ofPattern(format).parse(value, LocalDate::from);
        } catch (final DateTimeParseException e) {
            throw new InvalidDateTimeException(value);
        }
    }

    public static String toString(final TemporalAccessor value) {

        if(value instanceof LocalDate) {
            return toString((LocalDate)value);
        } else if(value instanceof Instant) {
            return toString((Instant)value);
        } else {
            throw new InvalidDateTimeException(value);
        }
    }

    public static String toString(final Date value) {

        if(value instanceof java.sql.Date) {
            return toString(((java.sql.Date) value).toLocalDate());
        } else {
            return toString(value.toInstant());
        }
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

    public static Long toMillis(final Instant value) {

        return value.toEpochMilli();
    }

    public static Long toMillis(final LocalDate value) {

        return value.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
    }

    public static Long toMillis(final Date value) {

        if(value instanceof java.sql.Date) {
            return toMillis(((java.sql.Date) value).toLocalDate());
        } else {
            return toMillis(value.toInstant());
        }
    }

    @SuppressWarnings("deprecation")
    public static java.sql.Date toSqlDate(final LocalDate date) {

        // Deprecated, but seems more reliable than the other constructors, which end up with hrs, mins, secs embedded and breaks equals/hashcode
        return new java.sql.Date(date.getYear() - 1900, date.getMonthValue() - 1, date.getDayOfMonth());
    }

    public static java.sql.Timestamp toSqlTimestamp(final Instant instant) {

        return new java.sql.Timestamp(instant.toEpochMilli());
    }

    public static Instant now() {

        return Instant.now().truncatedTo(ChronoUnit.MILLIS);
    }
}
