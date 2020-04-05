package io.basestar.util;

import com.google.common.io.BaseEncoding;
import lombok.Data;

import java.io.Serializable;
import java.util.Arrays;

@Data
public class PagingToken implements Serializable {

    private static final BaseEncoding ENCODING = BaseEncoding.base64Url().omitPadding();

    private final byte[] value;

    public PagingToken(final byte[] value) {

        this.value = Arrays.copyOf(value, value.length);
    }

    public PagingToken(final String value) {

        this.value = ENCODING.decode(value);
    }

    @Override
    public String toString() {

        return ENCODING.encode(value);
    }
}
