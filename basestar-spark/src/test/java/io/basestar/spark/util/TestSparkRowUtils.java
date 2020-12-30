package io.basestar.spark.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.basestar.util.ISO8601;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestSparkRowUtils {

    @Test
    void testToSpark() {

        assertEquals(ScalaUtils.scalaSingletonMap("test", 1), SparkRowUtils.toSpark(ImmutableMap.of("test", 1)));
        assertEquals(ScalaUtils.asScalaSeq(ImmutableList.of("test")), SparkRowUtils.toSpark(ImmutableList.of("test")));
        assertTrue(SparkRowUtils.toSpark(LocalDate.now()) instanceof java.sql.Date);
        assertTrue(SparkRowUtils.toSpark(Instant.now()) instanceof java.sql.Timestamp);
    }

    @Test
    void testFromSpark() {

        assertEquals(ImmutableMap.of("test", 1), SparkRowUtils.fromSpark(ScalaUtils.scalaSingletonMap("test", 1)));
        assertEquals(ImmutableList.of("test"), SparkRowUtils.fromSpark(ScalaUtils.asScalaSeq(ImmutableList.of("test"))));
        assertTrue(SparkRowUtils.fromSpark(ISO8601.toSqlDate(LocalDate.now())) instanceof LocalDate);
        assertTrue(SparkRowUtils.fromSpark(ISO8601.toSqlTimestamp(Instant.now())) instanceof Instant);
        final Row row = new GenericRowWithSchema(new Object[]{4L, 5L},
                DataTypes.createStructType(new StructField[]{
                        SparkRowUtils.field("x", DataTypes.LongType),
                        SparkRowUtils.field("y", DataTypes.LongType)
                }));
        assertEquals(ImmutableMap.of("x", 4L, "y", 5L), SparkRowUtils.fromSpark(row));
    }
}
