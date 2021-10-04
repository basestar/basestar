package io.basestar.util;

import com.google.common.base.Charsets;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;

public class BinaryKey extends Bytes {

    private static final BinaryKey EMPTY = new BinaryKey(new byte[0]);

    public static final byte[] LO_PREFIX = new byte[]{0};

    public static final byte[] HI_PREFIX = new byte[]{127};

    public static final byte T_NULL = 1;
    public static final byte T_FALSE = 2;
    public static final byte T_TRUE = 3;
    public static final byte T_INT = 4;
    public static final byte T_STRING = 5;
    public static final byte T_DATE = 6;
    public static final byte T_DATETIME = 7;
    public static final byte T_BYTES = 8;

    // Reserve bytes underneath escape for new control codes
    public static final int T_ESCAPE = 16;

    public BinaryKey(final byte[] bytes) {

        super(bytes);
    }

    public static BinaryKey empty() {

        return EMPTY;
    }

    public BinaryKey lo() {

        return new BinaryKey(concat(getBytes(), LO_PREFIX));
    }

    public BinaryKey hi() {

        return new BinaryKey(concat(getBytes(), HI_PREFIX));
    }

    public BinaryKey concat(final BinaryKey other) {

        return new BinaryKey(concat(getBytes(), other.getBytes()));
    }

    public static BinaryKey from(final List<?> keys) {

        return new BinaryKey(bytes(keys));
    }

    private static byte[] bytes(final List<?> keys) {

        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {

            for (final Object v : keys) {
                if (v == null) {
                    baos.write(T_NULL);
                } else if (v instanceof Boolean) {
                    baos.write(((Boolean) v) ? T_TRUE : T_FALSE);
                } else if (v instanceof Byte || v instanceof Short || v instanceof Integer || v instanceof Long) {
                    final byte[] bytes = longBytes((Number) v);
                    baos.write(T_INT);
                    baos.write(bytes);
                } else if (v instanceof Character) {
                    baos.write(T_STRING);
                    baos.write(stringBytes(String.valueOf(v)));
                } else if (v instanceof String) {
                    baos.write(T_STRING);
                    baos.write(stringBytes((String) v));
                } else if (v instanceof LocalDate) {
                    final byte[] bytes = dateBytes((LocalDate) v);
                    baos.write(T_DATE);
                    baos.write(bytes);
                } else if (v instanceof Instant) {
                    final byte[] bytes = datetimeBytes((Instant) v);
                    baos.write(T_DATETIME);
                    baos.write(bytes);
                } else if (v instanceof Bytes) {
                    baos.write(T_BYTES);
                    baos.write(genericBytes(((Bytes) v).getBytes()));
                } else if (v instanceof byte[]) {
                    baos.write(T_BYTES);
                    baos.write(genericBytes((byte[]) v));
                } else {
                    throw new IllegalStateException("Cannot convert " + v.getClass() + " to binary");
                }
            }

            return baos.toByteArray();

        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static byte[] concat(final byte[]... arrays) {

        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {

            for (final byte[] array : arrays) {
                baos.write(array);
            }

            return baos.toByteArray();

        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static byte[] dateBytes(final LocalDate v) {

        return datetimeBytes(v.atStartOfDay().toInstant(ZoneOffset.UTC));
    }

    private static byte[] datetimeBytes(final Instant v) {

        return longBytes(v.toEpochMilli());
    }

    private static byte[] longBytes(final Number v) {

        final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(v.longValue());
        return buffer.array();
    }

    private static byte[] stringBytes(final String str) throws IOException {

        return genericBytes(str.getBytes(Charsets.UTF_8));
    }

    private static byte[] genericBytes(final byte[] bytes) throws IOException {

        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {

            for (final byte b : bytes) {
                if (b <= T_ESCAPE) {
                    baos.write(T_ESCAPE);
                }
                baos.write(b);
            }
            return baos.toByteArray();
        }
    }
}