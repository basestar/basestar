package io.basestar.schema.use;

/*-
 * #%L
 * basestar-schema
 * %%
 * Copyright (C) 2019 - 2020 Basestar.IO
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.google.common.base.Charsets;
import com.google.common.io.BaseEncoding;
import io.basestar.schema.exception.UnexpectedTypeException;
import io.basestar.util.Name;
import io.swagger.v3.oas.models.media.BinarySchema;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;

/**
 * Binary Type
 *
 * Input/output as a Base64 encoded string
 *
 * <strong>Example</strong>
 * <pre>
 * type: binary
 * </pre>
 */

@Data
@Slf4j
public class UseBinary implements UseScalar<byte[]> {

    public static final UseBinary DEFAULT = new UseBinary();

    public static final byte[] LO_PREFIX = new byte[]{0};

    public static final byte[] HI_PREFIX = new byte[]{127};

    public static final String NAME = "binary";

    @Override
    public <R> R visit(final Visitor<R> visitor) {

        return visitor.visitBinary(this);
    }

    public static UseBinary from(final Object config) {

        return DEFAULT;
    }

    @Override
    public Object toConfig(final boolean optional) {

        return Use.name(NAME, optional);
    }

    @Override
    public byte[] create(final Object value, final Set<Name> expand, final boolean suppress) {

        if(value == null) {
            return null;
        } else if(value instanceof byte[]) {
            return (byte[])value;
        } else if(value instanceof ByteBuffer) {
            return ((ByteBuffer) value).array();
        } else if(value instanceof String) {
            return BaseEncoding.base64().decode((String)value);
        } else if(suppress) {
            return null;
        } else {
            throw new UnexpectedTypeException(this, value);
        }
    }

    @Override
    public Code code() {

        return Code.BINARY;
    }

    @Override
    public Type javaType(final Name name) {

        return byte[].class;
    }

    @Override
    public byte[] defaultValue() {

        return new byte[0];
    }

    @Override
    public io.swagger.v3.oas.models.media.Schema<?> openApi(final Set<Name> expand) {

        return new BinarySchema();
    }

    @Override
    public void serializeValue(final byte[] value, final DataOutput out) throws IOException {

        out.writeInt(value.length);
        out.write(value);
    }

    @Override
    public byte[] deserializeValue(final DataInput in) throws IOException {

        final int size = in.readInt();
        final byte[] buffer = new byte[size];
        in.readFully(buffer);
        return buffer;
    }

    @Override
    public String toString() {

        return NAME;
    }

    public static byte[] binaryKey(final List<?> keys) {

        return binaryKey(keys, null);
    }

    public static byte[] binaryKey(final List<?> keys, final byte[] suffix) {

        final byte T_NULL = 1;
        final byte T_FALSE = 2;
        final byte T_TRUE = 3;
        final byte T_INT = 4;
        final byte T_STRING = 5;
        final byte T_DATE = 6;
        final byte T_DATETIME = 7;
        final byte T_BYTES = 8;

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try {
            for(final Object v : keys) {
                if(v == null) {
                    baos.write(T_NULL);
                } else if(v instanceof Boolean) {
                    baos.write(((Boolean)v) ? T_TRUE : T_FALSE);
                } else if(v instanceof Integer || v instanceof Long) {
                    final byte[] bytes = longBytes((Number)v);
                    baos.write(T_INT);
                    baos.write(bytes);
                } else if(v instanceof String) {
                    baos.write(T_STRING);
                    baos.write(stringBytes((String)v));
                } else if(v instanceof LocalDate) {
                    final byte[] bytes = dateBytes((LocalDate)v);
                    baos.write(T_DATE);
                    baos.write(bytes);
                } else if(v instanceof Instant) {
                    final byte[] bytes = datetimeBytes((Instant)v);
                    baos.write(T_DATETIME);
                    baos.write(bytes);
                } else if(v instanceof byte[]) {
                    baos.write(T_BYTES);
                    baos.write(((byte[]) v));
                } else {
                    throw new IllegalStateException("Cannot convert " + v.getClass() + " to binary");
                }
            }

            if(suffix != null) {
                baos.write(suffix);
            }

        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }

        return baos.toByteArray();
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

    private static byte[] stringBytes(final String str) {

        if(str.contains("\0")) {
            throw new IllegalStateException("String used in index cannot contain NULL byte");
        }
        return str.getBytes(Charsets.UTF_8);
    }
}
