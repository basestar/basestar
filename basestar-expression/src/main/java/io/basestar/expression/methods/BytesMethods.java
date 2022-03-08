package io.basestar.expression.methods;

import io.basestar.util.Bytes;

import java.io.Serializable;

public class BytesMethods implements Serializable {

    public String upperhex(final Bytes value) {

        return value == null ? null : value.toUpperHex();
    }

    public String lowerhex(final Bytes value) {

        return value == null ? null : value.toLowerHex();
    }

    public String base64(final Bytes value) {

        return value == null ? null : value.toBase64();
    }

    public String base32(final Bytes value) {

        return value == null ? null : value.toBase32();
    }

    public Bytes xorfold(final Bytes value, final Long length) {

        return value == null || length == null ? null : value.xorFold(length.intValue());
    }
}
