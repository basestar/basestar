package io.basestar.util;

import com.google.common.io.BaseEncoding;

import java.util.Arrays;

@SuppressWarnings(Warnings.GUAVA)
public class Bytes {

    private static final Bytes EMPTY = new Bytes();

    private final byte[] bytes;

    public Bytes() {

        this.bytes = new byte[0];
    }

    public Bytes(final byte [] bytes) {

        this.bytes = Arrays.copyOf(bytes, bytes.length);
    }

    public static Bytes empty() {

        return EMPTY;
    }

    public static Bytes valueOf(final int ... bytes) {

        if(bytes.length == 0) {
            return EMPTY;
        } else {
            final byte[] tmp = new byte[bytes.length];
            for(int i = 0; i != bytes.length; ++i) {
                tmp[i] = (byte)bytes[i];
            }
            return new Bytes(tmp);
        }
    }

    public static Bytes valueOf(final byte [] bytes) {

        if(bytes.length == 0) {
            return EMPTY;
        } else {
            return new Bytes(bytes);
        }
    }

    public String toBase64() {

        return BaseEncoding.base64().encode(bytes);
    }

    public static Bytes fromBase64(final String str) {

        return new Bytes(BaseEncoding.base64().decode(str));
    }

    public byte[] getBytes() {

        return bytes;
    }

    public int length() {

        return bytes.length;
    }

    @Override
    public String toString() {

        return toBase64();
    }

    @Override
    public boolean equals(final Object o) {

        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Bytes bytes1 = (Bytes) o;
        return Arrays.equals(bytes, bytes1.bytes);
    }

    @Override
    public int hashCode() {

        return Arrays.hashCode(bytes);
    }
}
