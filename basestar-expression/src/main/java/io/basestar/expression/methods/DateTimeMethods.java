package io.basestar.expression.methods;

import io.basestar.date.DateUnit;
import io.basestar.util.ISO8601;

import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.TemporalAccessor;

@SuppressWarnings("unused")
public class DateTimeMethods extends TemporalAccessorMethods<Instant> {

    public Instant date_add(final String unit, final Long amount, final TemporalAccessor value) {

        return (unit == null || amount == null || value == null) ? null : DateUnit.from(unit).add(amount, value);
    }

    public Long date_diff(final String unit, final TemporalAccessor a, final TemporalAccessor b) {

        return (unit == null || a == null || b == null) ? null : DateUnit.from(unit).between(a, b);
    }

    public Instant date_trunc(final String unit, final TemporalAccessor value) {

        return (unit == null || value == null) ? null : DateUnit.from(unit).trunc(value);
    }

    public LocalDate from_iso8601_date(final String str) {

        return (str == null) ? null : ISO8601.parseDate(str);
    }

    public Instant from_iso8601_timestamp(final String str) {

        return (str == null) ? null : ISO8601.parseDateTime(str);
    }
}
