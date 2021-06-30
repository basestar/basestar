package io.basestar.spark.util;

import io.basestar.util.ISO8601;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.UDFRegistration;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataTypes;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.Date;

public class CompatibilityUDFs {

    interface DateFunctions {

        Timestamp add(Long amount, Date value);

        Long between(Date a, Date b);

        Timestamp trunc(Date value);

        class Simple implements DateFunctions {

            private final ChronoUnit unit;

            public Simple(final ChronoUnit unit) {

                this.unit = unit;
            }

            @Override
            public Timestamp add(final Long amount, final Date value) {

                final Instant before = ISO8601.toDateTime(value);
                final Instant after = before.plus(amount, unit);
                return ISO8601.toSqlTimestamp(after);
            }

            @Override
            public Long between(final Date a, final Date b) {

                return unit.between(ISO8601.toDateTime(a), ISO8601.toDateTime(b));
            }

            @Override
            public Timestamp trunc(final Date value) {

                return ISO8601.toSqlTimestamp(ISO8601.toDateTime(value)
                        .truncatedTo(unit));
            }
        }

        class Quarter implements DateFunctions {

            @Override
            public Timestamp add(final Long amount, final Date value) {

                final Instant before = ISO8601.toDateTime(value);
                final Instant after = before.plus(amount * 91, ChronoUnit.DAYS);
                return ISO8601.toSqlTimestamp(after);
            }

            @Override
            public Long between(final Date a, final Date b) {

                return ChronoUnit.DAYS.between(ISO8601.toDateTime(a), ISO8601.toDateTime(b)) / 91;
            }

            @Override
            public Timestamp trunc(final Date value) {

                final LocalDate before = ISO8601.toDate(value);
                final LocalDate after = before.with(before.getMonth().firstMonthOfQuarter())
                        .with(TemporalAdjusters.firstDayOfMonth());
                return ISO8601.toSqlTimestamp(ISO8601.toDateTime(after));
            }
        }
    }

    enum DateUnit implements DateFunctions {

        MILLISECOND(new DateFunctions.Simple(ChronoUnit.MILLIS)),
        SECOND(new DateFunctions.Simple(ChronoUnit.SECONDS)),
        MINUTE(new DateFunctions.Simple(ChronoUnit.MINUTES)),
        HOUR(new DateFunctions.Simple(ChronoUnit.HOURS)),
        DAY(new DateFunctions.Simple(ChronoUnit.DAYS)),
        WEEK(new DateFunctions.Simple(ChronoUnit.WEEKS)),
        MONTH(new DateFunctions.Simple(ChronoUnit.MONTHS)),
        QUARTER(new DateFunctions.Quarter()),
        YEAR(new DateFunctions.Simple(ChronoUnit.YEARS));

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
        public Timestamp add(final Long amount, final Date value) {

            return delegate.add(amount, value);
        }

        @Override
        public Long between(final Date a, final Date b) {

            return delegate.between(a, b);
        }

        @Override
        public Timestamp trunc(final Date value) {

            return delegate.trunc(value);
        }
    }

    public static void register(final SparkSession session) {

        final UDFRegistration udfs = session.sqlContext().udf();

        udfs.register("date_add",
                (UDF3<String, Long, Date, Timestamp>) (unit, amount, value) -> (unit == null || amount == null || value == null) ? null : DateUnit.from(unit).add(amount, value),
                DataTypes.TimestampType);

        udfs.register("date_diff",
                (UDF3<String, Date, Date, Long>) (unit, a, b) -> (unit == null || a == null || b == null) ? null : DateUnit.from(unit).between(a, b), DataTypes.LongType);

        udfs.register("date_trunc",
                (UDF2<String, Date, Timestamp>) (unit, value) -> (unit == null || value == null) ? null : DateUnit.from(unit).trunc(value), DataTypes.TimestampType);

        udfs.register("from_iso8601_date",
                (UDF1<String, java.sql.Date>) str -> (str == null) ? null : ISO8601.toSqlDate(ISO8601.parseDate(str)),
                DataTypes.DateType);

        udfs.register("from_iso8601_timestamp",
                (UDF1<String, Timestamp>) str -> (str == null) ? null : ISO8601.toSqlTimestamp(ISO8601.parseDateTime(str)),
                DataTypes.TimestampType);
    }

}
