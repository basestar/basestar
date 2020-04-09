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
import io.basestar.schema.exception.InvalidTypeException;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * String Type
 *
 * <strong>Example</strong>
 * <pre>
 * type: string
 * </pre>
 */

@Data
@RequiredArgsConstructor
public class UseString implements UseScalar<String> {

    public static final UseString DEFAULT = new UseString(null);

    public static final String NAME = "string";

    private final String pattern;

    public UseString() {

        this(null);
    }

    public static UseString from(final Object config) {

        if(config == null) {
            return DEFAULT;
        } else if(config instanceof String) {
            return new UseString((String)config);
        } else if(config instanceof Map) {
            return new UseString((String)((Map)config).get("pattern"));
        } else {
            throw new InvalidTypeException();
        }
    }

    @Override
    public <R> R visit(final Visitor<R> visitor) {

        return visitor.visitString(this);
    }

    @Override
    public Object toJson() {

        return NAME;
    }

    @Override
    public String create(final Object value) {

        if(value == null) {
            return null;
        } else if(value instanceof Boolean || value instanceof Number) {
            return value.toString();
        } else if(value instanceof String) {
            return (String)value;
        } else {
            throw new InvalidTypeException();
        }
    }

    @Override
    public Code code() {

        return Code.STRING;
    }

    @Override
    public void serializeValue(final String value, final DataOutput out) throws IOException {

        final byte[] buffer = value.getBytes(Charsets.UTF_8);
        out.writeInt(buffer.length);
        out.write(buffer);
    }

    @Override
    public String deserializeValue(final DataInput in) throws IOException {

        final int length = in.readInt();
        final byte[] buffer = new byte[length];
        in.readFully(buffer);
        return new String(buffer, Charsets.UTF_8);
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
