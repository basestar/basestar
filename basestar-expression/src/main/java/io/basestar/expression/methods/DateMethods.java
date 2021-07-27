package io.basestar.expression.methods;

import io.basestar.util.ISO8601;

import java.time.LocalDate;
import java.time.temporal.ChronoField;
import java.time.temporal.IsoFields;

@SuppressWarnings("unused")
public class DateMethods extends TemporalAccessorMethods<LocalDate> {

    public LocalDate _date(final Object any) {

        return ISO8601.toDate(any);
    }

    public LocalDate _to_date(final String str, final String format) {

        return ISO8601.parseDate(str, format);
    }

    public LocalDate _last_day(final Object value) {

        final LocalDate initial = ISO8601.toDate(value);
        return initial.withDayOfMonth(initial.lengthOfMonth());
    }

    public Long _year(final Object value) {

        final LocalDate date = ISO8601.toDate(value);
        return (long) date.getYear();
    }

    public Long _month(final Object value) {

        final LocalDate date = ISO8601.toDate(value);
        return (long) date.get(ChronoField.MONTH_OF_YEAR);
    }

    public Long _day(final Object value) {

        final LocalDate date = ISO8601.toDate(value);
        return (long) date.getDayOfMonth();
    }

    public Long _quarter(final Object value) {

        final LocalDate date = ISO8601.toDate(value);
        return (long) date.get(IsoFields.QUARTER_OF_YEAR);
    }
}
