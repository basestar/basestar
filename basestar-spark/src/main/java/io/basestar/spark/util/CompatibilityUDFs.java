package io.basestar.spark.util;

import io.basestar.date.DateUnit;
import io.basestar.util.ISO8601;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.UDFRegistration;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataTypes;

import java.sql.Timestamp;
import java.util.Date;

public class CompatibilityUDFs {

    public static void register(final SparkSession session) {

        final UDFRegistration udfs = session.sqlContext().udf();

        udfs.register("date_add",
                (UDF3<String, Number, Date, Timestamp>) (unit, amount, value) -> (unit == null || amount == null || value == null) ? null : ISO8601.toSqlTimestamp(DateUnit.from(unit).add(amount.longValue(), ISO8601.toDateTime(value))),
                DataTypes.TimestampType);

        udfs.register("date_diff",
                (UDF3<String, Date, Date, Long>) (unit, a, b) -> (unit == null || a == null || b == null) ? null : DateUnit.from(unit).between(ISO8601.toDateTime(a), ISO8601.toDateTime(b)),
                DataTypes.LongType);

        udfs.register("date_trunc",
                (UDF2<String, Date, Timestamp>) (unit, value) -> (unit == null || value == null) ? null : ISO8601.toSqlTimestamp(DateUnit.from(unit).trunc(ISO8601.toDateTime(value))),
                DataTypes.TimestampType);

        udfs.register("from_iso8601_date",
                (UDF1<String, java.sql.Date>) str -> (str == null) ? null : ISO8601.toSqlDate(ISO8601.parseDate(str)),
                DataTypes.DateType);

        udfs.register("from_iso8601_timestamp",
                (UDF1<String, Timestamp>) str -> (str == null) ? null : ISO8601.toSqlTimestamp(ISO8601.parseDateTime(str)),
                DataTypes.TimestampType);
    }
}
