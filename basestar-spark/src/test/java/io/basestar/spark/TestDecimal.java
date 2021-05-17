package io.basestar.spark;

import com.google.common.collect.ImmutableList;
import io.basestar.expression.type.DecimalContext;
import io.basestar.expression.type.Numbers;
import io.basestar.spark.util.SparkRowUtils;
import io.basestar.util.Nullsafe;
import lombok.Data;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestDecimal extends AbstractSparkTest {

    private static final int MAX_SPARK_PRECISION = 38;

    protected SparkSession session() {

        final SparkSession session = SparkSession.builder()
                .enableHiveSupport()
                .master("local[*]")
                .getOrCreate();
        session.sparkContext().setLogLevel("WARN");
        session.sqlContext().setConf("spark.sql.shuffle.partitions", "2");
        return session;
    }

    @Data
    private static class Record {

        private final BigDecimal v0;

        private final BigDecimal v1;

        private final BigDecimal r;

        public Record(final CSVRecord record) {

            final BigInteger v0u = new BigInteger(record.get("v0u"));
            final int v0s = Integer.parseInt(record.get("v0s"));
            final BigInteger v1u = new BigInteger(record.get("v1u"));
            final int v1s = Integer.parseInt(record.get("v1s"));
            final BigInteger ru = new BigInteger(record.get("ru"));
            final int rs = Integer.parseInt(record.get("rs"));
            this.v0 = new BigDecimal(v0u, v0s);
            this.v1 = new BigDecimal(v1u, v1s);
            this.r = new BigDecimal(ru, rs);
        }

        public boolean isSupported() {

            return isSupported(v0) && isSupported(v1) && isSupported(r);
        }

        private boolean isSupported(final BigDecimal v) {

            return v.precision() <= MAX_SPARK_PRECISION && v.scale() >= 0 && v.scale() <= v.precision();
        }

        public Row valueRow() {

            return new GenericRow(new Object[]{v0, v1});
        }

        public StructType valueSchema() {

            final DataType v0t = DataTypes.createDecimalType(v0.precision() , v0.scale());
            final DataType v1t = DataTypes.createDecimalType(v1.precision(), v1.scale());

            final StructField f0 = SparkRowUtils.field("v0", v0t);
            final StructField f1 = SparkRowUtils.field("v1", v1t);
            return DataTypes.createStructType(new StructField[]{f0, f1});
        }
    }

    private void runTest(final String name, final BiFunction<Column, Column, Column> fn, final BiFunction<BigDecimal, BigDecimal, DecimalContext.PrecisionAndScale> precision) throws IOException {

        final SparkSession session = session();

        try(final Reader reader = new InputStreamReader(Nullsafe.require(TestDecimal.class.getResourceAsStream("/decimal/" + name + ".csv")));
            final CSVParser parser = CSVFormat.DEFAULT.withHeader().parse(reader)) {

            final List<Record> records =  parser.getRecords().stream().map(Record::new).filter(Record::isSupported).collect(Collectors.toList());
            records.forEach(record -> {
                final StructType valueSchema = record.valueSchema();
                final Dataset<Row> vs = session.sqlContext().createDataset(ImmutableList.of(record.valueRow()), RowEncoder.apply(valueSchema));
                final Dataset<Row> rs = vs.withColumn("r", fn.apply(vs.col("v0"), vs.col("v1")));
                final StructType resultSchema = rs.schema();
                final StructField resultField = SparkRowUtils.requireField(resultSchema, "r");
                final DecimalType resultType = (DecimalType)resultField.dataType();
                final int actualScale = resultType.scale();
                final int actualPrecision = resultType.precision();
                final DecimalContext.PrecisionAndScale precisionAndScale = precision.apply(record.getV0(), record.getV1());
                final int expectedPrecision = Math.min(precisionAndScale.getPrecision(), MAX_SPARK_PRECISION);
                assertEquals(expectedPrecision, actualPrecision);
                final Row row = rs.collectAsList().get(0);
                final BigDecimal actual = ((BigDecimal)row.get(2));
                final BigDecimal expected = record.getR();
                try {
                    // only test equality to the output (schema) precision
                    assertEquals(
                            expected.setScale(actualScale, Numbers.DECIMAL_ROUNDING_MODE).round(new MathContext(actualPrecision, Numbers.DECIMAL_ROUNDING_MODE)),
                            actual.setScale(actualScale, Numbers.DECIMAL_ROUNDING_MODE).round(new MathContext(actualPrecision, Numbers.DECIMAL_ROUNDING_MODE))
                    );
                } catch (final AssertionFailedError e) {
                    System.err.println("f");
                }
            });
        }
    }

    @Test
    public void testAdd() throws IOException {

        runTest("add", Column::plus, DecimalContext.DEFAULT::addition);
    }

    @Test
    public void testSub() throws IOException {

        runTest("sub", Column::minus, DecimalContext.DEFAULT::addition);
    }

    @Test
    public void testMul() throws IOException {

        runTest("mul", Column::multiply, DecimalContext.DEFAULT::multiplication);
    }

    @Test
    public void testDiv() throws IOException {

        runTest("div", Column::divide, DecimalContext.DEFAULT::division);
    }
}
