package io.basestar.util;

import com.google.common.io.BaseEncoding;
import lombok.Data;

@Data
public class Bytes {

    private final byte[] bytes;

    public Bytes(final byte[] bytes) {

        this.bytes = bytes;
    }

    public String toString() {

        return BaseEncoding.base64().encode(bytes);
    }
}
