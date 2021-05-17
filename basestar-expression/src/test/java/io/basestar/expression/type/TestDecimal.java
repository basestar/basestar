package io.basestar.expression.type;

import io.basestar.expression.Context;
import io.basestar.expression.arithmetic.Add;
import io.basestar.expression.arithmetic.Div;
import io.basestar.expression.arithmetic.Mul;
import io.basestar.expression.arithmetic.Sub;
import io.basestar.expression.constant.Constant;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Paths;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestDecimal {

    private static final boolean GENERATE = true;

    private void generateOrRunTest(final String name, final BiFunction<BigDecimal, BigDecimal, BigDecimal> fn) throws IOException {

        final String csvPath = Paths.get("../basestar-expression/src/test/resources/decimal/" + name + ".csv").toAbsolutePath().normalize().toString();

        if(GENERATE) {
            try(final Writer writer = new FileWriter(csvPath);
                final CSVPrinter printer = CSVFormat.DEFAULT.withHeader(new String[]{"v0u", "v0s", "v1u", "v1s", "ru", "rs"}).print(writer)) {

                for (int v0s = -10; v0s != 10; ++v0s) {
                    for (int v0p = 1; v0p != 5; ++v0p) {
                        for (int v1s = -10; v1s != 10; ++v1s) {
                            for (int v1p = 1; v1p != 5; ++v1p) {
                                final BigDecimal v0 = new BigDecimal(BigInteger.valueOf(2).pow(v0p * 10), v0s);
                                final BigDecimal v1 = new BigDecimal(BigInteger.valueOf(3).pow(v1p * 10), v1s);
                                try {
                                    final BigDecimal r = fn.apply(v0, v1);
                                    printer.printRecord(v0.unscaledValue(), v0.scale(), v1.unscaledValue(), v1.scale(), r.unscaledValue(), r.scale());
                                } catch (final ArithmeticException e) {
                                    System.err.println("cannot output " + v0 + " / " + v1 + " (" + e.getMessage() + ")");
                                }
                            }
                        }
                    }
                }
            }
        } else {

            try(final Reader reader = new FileReader(csvPath);
                final CSVParser parser = CSVFormat.DEFAULT.withHeader().parse(reader)) {

                parser.getRecords().forEach(record -> {
                    final BigInteger v0u = new BigInteger(record.get("v0u"));
                    final int v0s = Integer.parseInt(record.get("v0s"));
                    final BigInteger v1u = new BigInteger(record.get("v1u"));
                    final int v1s = Integer.parseInt(record.get("v1s"));
                    final BigInteger ru = new BigInteger(record.get("ru"));
                    final int rs = Integer.parseInt(record.get("rs"));
                    final BigDecimal v0 = new BigDecimal(v0u, v0s);
                    final BigDecimal v1 = new BigDecimal(v1u, v1s);
                    final BigDecimal expected = new BigDecimal(ru, rs);
                    final BigDecimal actual = fn.apply(v0, v1);
                    assertEquals(expected, actual);
                    assertEquals(expected.unscaledValue(), actual.unscaledValue());
                    assertEquals(expected.precision(), actual.precision());
                    assertEquals(expected.scale(), actual.scale());
                });
            }
        }
    }

    @Test
    public void testAdd() throws IOException {

        generateOrRunTest("add", (v0, v1) -> new Add(Constant.valueOf(v0), Constant.valueOf(v1)).evaluateAs(BigDecimal.class, Context.init()));
    }

    @Test
    public void testSub() throws IOException {

        generateOrRunTest("sub", (v0, v1) -> new Sub(Constant.valueOf(v0), Constant.valueOf(v1)).evaluateAs(BigDecimal.class, Context.init()));
    }

    @Test
    public void testMul() throws IOException {

        generateOrRunTest("mul", (v0, v1) -> new Mul(Constant.valueOf(v0), Constant.valueOf(v1)).evaluateAs(BigDecimal.class, Context.init()));
    }

    @Test
    public void testDiv() throws IOException {

        generateOrRunTest("div", (v0, v1) -> new Div(Constant.valueOf(v0), Constant.valueOf(v1)).evaluateAs(BigDecimal.class, Context.init()));
    }
}
