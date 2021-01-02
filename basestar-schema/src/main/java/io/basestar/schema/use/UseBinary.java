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

import io.basestar.schema.util.ValueContext;
import io.basestar.util.Bytes;
import io.basestar.util.Name;
import io.swagger.v3.oas.models.media.BinarySchema;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Type;
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
public class UseBinary implements UseScalar<Bytes> {

    public static final UseBinary DEFAULT = new UseBinary();

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
    public Bytes create(final ValueContext context, final Object value, final Set<Name> expand) {

        return context.createBinary(this, value, expand);
    }

    @Override
    public Code code() {

        return Code.BINARY;
    }

    @Override
    public Type javaType(final Name name) {

        return Bytes.class;
    }

    @Override
    public Bytes defaultValue() {

        return new Bytes();
    }

    @Override
    public io.swagger.v3.oas.models.media.Schema<?> openApi(final Set<Name> expand) {

        return new BinarySchema();
    }

    @Override
    public void serializeValue(final Bytes value, final DataOutput out) throws IOException {

        out.writeInt(value.length());
        out.write(value.getBytes());
    }

    @Override
    public Bytes deserializeValue(final DataInput in) throws IOException {

        final int size = in.readInt();
        final byte[] buffer = new byte[size];
        in.readFully(buffer);
        return new Bytes(buffer);
    }

    @Override
    public String toString() {

        return NAME;
    }

    @Override
    public String toString(final Bytes value) {

        return value == null ? "null" : value.toString();
    }

    @Override
    public boolean areEqual(final Bytes a, final Bytes b) {

        if(a == null || b == null) {
            return a == null && b == null;
        } else {
            return a.equals(b);
        }
    }
}
