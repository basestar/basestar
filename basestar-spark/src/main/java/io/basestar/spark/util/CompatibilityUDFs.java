package io.basestar.spark.util;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.UDFRegistration;

public class CompatibilityUDFs {

    public static void register(final SparkSession session) {

        final UDFRegistration udfs = session.sqlContext().udf();

//        udfs.register("date_add",
//                (UDF3<String, Long, Date, Timestamp>) (unit, amount, value) -> (unit == null || amount == null || value == null) ? null : DateUnit.from(unit).add(amount, value),
//                DataTypes.TimestampType);
//
//        udfs.register("date_diff",
//                (UDF3<String, Date, Date, Long>) (unit, a, b) -> (unit == null || a == null || b == null) ? null : DateUnit.from(unit).between(a, b), DataTypes.LongType);
//
//        udfs.register("date_trunc",
//                (UDF2<String, Date, Timestamp>) (unit, value) -> (unit == null || value == null) ? null : DateUnit.from(unit).trunc(value), DataTypes.TimestampType);
//
//        udfs.register("from_iso8601_date",
//                (UDF1<String, java.sql.Date>) str -> (str == null) ? null : ISO8601.toSqlDate(ISO8601.parseDate(str)),
//                DataTypes.DateType);
//
//        udfs.register("from_iso8601_timestamp",
//                (UDF1<String, Timestamp>) str -> (str == null) ? null : ISO8601.toSqlTimestamp(ISO8601.parseDateTime(str)),
//                DataTypes.TimestampType);
    }

}
