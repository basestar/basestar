package io.basestar.schema.encoding;

import io.basestar.encoding.Encoding;

import java.util.Map;

public class BinaryEncoding implements Encoding<Map<String, Object>, byte[]> {

    @Override
    public byte[] encode(final Map<String, Object> v) {

        return new byte[0];
    }

    @Override
    public Map<String, Object> decode(final byte[] v) {

        return null;
    }
}
