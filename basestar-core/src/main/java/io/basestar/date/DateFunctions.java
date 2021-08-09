package io.basestar.date;

import io.basestar.util.ISO8601;

import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalAdjusters;

public interface DateFunctions {

    Instant add(Long amount, TemporalAccessor value);

    Long between(TemporalAccessor a, TemporalAccessor b);

    Instant trunc(TemporalAccessor value);

    class Simple implements DateFunctions {

        private final ChronoUnit unit;

        public Simple(final ChronoUnit unit) {

            this.unit = unit;
        }

        @Override
        public Instant add(final Long amount, final TemporalAccessor value) {

            final Instant before = ISO8601.toDateTime(value);
            return before.plus(amount, unit);
        }

        @Override
        public Long between(final TemporalAccessor a, final TemporalAccessor b) {

            return unit.between(ISO8601.toDateTime(a), ISO8601.toDateTime(b));
        }

        @Override
        public Instant trunc(final TemporalAccessor value) {

            return ISO8601.toDateTime(value)
                    .truncatedTo(unit);
        }
    }

    class Quarter implements DateFunctions {

        @Override
        public Instant add(final Long amount, final TemporalAccessor value) {

            final Instant before = ISO8601.toDateTime(value);
            return before.plus(amount * 91, ChronoUnit.DAYS);
        }

        @Override
        public Long between(final TemporalAccessor a, final TemporalAccessor b) {

            return ChronoUnit.DAYS.between(ISO8601.toDateTime(a), ISO8601.toDateTime(b)) / 91;
        }

        @Override
        public Instant trunc(final TemporalAccessor value) {

            final LocalDate before = ISO8601.toDate(value);
            final LocalDate after = before.with(before.getMonth().firstMonthOfQuarter())
                    .with(TemporalAdjusters.firstDayOfMonth());
            return ISO8601.toDateTime(after);
        }
    }
}
