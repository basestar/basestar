package io.basestar.expression.methods;

import io.basestar.util.ISO8601;

import java.time.LocalDate;
import java.time.temporal.TemporalAccessor;

@SuppressWarnings("unused")
public class DateMethods extends TemporalAccessorMethods<LocalDate> {

    public LocalDate date(final Object any) {

        return ISO8601.toDate(any);
    }

    public LocalDate to_date(final String str, final String format) {

        return ISO8601.parseDate(str, format);
    }

    public LocalDate last_day(final TemporalAccessor value) {

        final LocalDate initial = ISO8601.toDate(value);
        return initial.withDayOfMonth(initial.lengthOfMonth());
    }
}
