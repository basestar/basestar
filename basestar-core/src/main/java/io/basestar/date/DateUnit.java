package io.basestar.date;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;

public enum DateUnit implements DateFunctions {

    MILLISECOND(new Simple(ChronoUnit.MILLIS)),
    SECOND(new Simple(ChronoUnit.SECONDS)),
    MINUTE(new Simple(ChronoUnit.MINUTES)),
    HOUR(new Simple(ChronoUnit.HOURS)),
    DAY(new Simple(ChronoUnit.DAYS)),
    WEEK(new Simple(ChronoUnit.WEEKS)),
    MONTH(new Simple(ChronoUnit.MONTHS)),
    QUARTER(new Quarter()),
    YEAR(new Simple(ChronoUnit.YEARS));

    private final DateFunctions delegate;

    DateUnit(final DateFunctions delegate) {

        this.delegate = delegate;
    }

    public static DateUnit from(final String unit) {

        try {
            return DateUnit.valueOf(unit.toUpperCase());
        } catch (final IllegalArgumentException e) {
            throw new IllegalArgumentException("Date unit " + unit + " is not supported");
        }
    }

    @Override
    public Instant add(final Long amount, final TemporalAccessor value) {

        return delegate.add(amount, value);
    }

    @Override
    public Long between(final TemporalAccessor a, final TemporalAccessor b) {

        return delegate.between(a, b);
    }

    @Override
    public Instant trunc(final TemporalAccessor value) {

        return delegate.trunc(value);
    }
}
