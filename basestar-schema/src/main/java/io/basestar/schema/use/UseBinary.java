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

import com.google.common.io.BaseEncoding;
import io.basestar.schema.exception.InvalidTypeException;
import lombok.Data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

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
public class UseBinary implements UseScalar<byte[]> {

    public static UseBinary DEFAULT = new UseBinary();

    public static final String NAME = "binary";

    @Override
    public <R> R visit(final Visitor<R> visitor) {

        return visitor.visitBinary(this);
    }

    public static UseBinary from(final Object config) {

        return new UseBinary();
    }

    @Override
    public Object toJson() {

        return NAME;
    }

    @Override
    public byte[] create(final Object value) {

        if(value == null) {
            return null;
        } else if(value instanceof byte[]) {
            return (byte[])value;
        } else if(value instanceof String) {
            return BaseEncoding.base64().decode((String)value);
        } else {
            throw new InvalidTypeException();
        }
    }

    @Override
    public Code code() {

        return Code.BINARY;
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

//    @Override
//    public Map<String, Object> openApiType() {
//
//        return ImmutableMap.of(
//                "type", "string"
//        );
//    }
}
