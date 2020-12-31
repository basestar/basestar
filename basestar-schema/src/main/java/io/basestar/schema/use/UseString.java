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
import io.basestar.schema.util.ValueContext;
import io.basestar.util.Name;
import io.swagger.v3.oas.models.media.StringSchema;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Set;

/**
 * String Type
 *
 * <strong>Example</strong>
 * <pre>
 * type: string
 * </pre>
 */

@Data
@Slf4j
@RequiredArgsConstructor
public class UseString implements UseStringLike<String> {

    public static final UseString DEFAULT = new UseString();

    public static final String NAME = "string";

    public static UseString from(final Object config) {

        return DEFAULT;
    }

    @Override
    public <R> R visit(final Visitor<R> visitor) {

        return visitor.visitString(this);
    }

    @Override
    public Object toConfig(final boolean optional) {

        return Use.name(NAME, optional);
    }

    @Override
    public String create(final ValueContext context, final Object value, final Set<Name> expand) {

        return context.createString(this, value, expand);
    }

    @Override
    public Code code() {

        return Code.STRING;
    }

    @Override
    public Type javaType(final Name name) {

        return String.class;
    }

    @Override
    public String defaultValue() {

        return "";
    }

    @Override
    public io.swagger.v3.oas.models.media.Schema<?> openApi(final Set<Name> expand) {

        return new StringSchema();
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
}
