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
import io.basestar.util.Name;
import io.swagger.v3.oas.models.media.IntegerSchema;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Set;

/**
 * Integer Type
 *
 * Stored as 64bit (long) integer.
 *
 * <strong>Example</strong>
 * <pre>
 * type: integer
 * </pre>
 */

@Data
@Slf4j
public class UseInteger implements UseNumeric<Long> {

    public static final UseInteger DEFAULT = new UseInteger();

    public static final String NAME = "integer";

    @Override
    public <R> R visit(final Visitor<R> visitor) {

        return visitor.visitInteger(this);
    }

    public static UseInteger from(final Object config) {

        return DEFAULT;
    }

    @Override
    public Object toConfig(final boolean optional) {

        return Use.name(NAME, optional);
    }

    @Override
    public Long create(final ValueContext context, final Object value, final Set<Name> expand) {

        return context.createInteger(this, value, expand);
    }

    @Override
    public Code code() {

        return Code.INTEGER;
    }

    @Override
    public Type javaType(final Name name) {

        return Long.class;
    }

    @Override
    public Long defaultValue() {

        return 0L;
    }

    @Override
    public io.swagger.v3.oas.models.media.Schema<?> openApi(final Set<Name> expand) {

        return new IntegerSchema();
    }

    @Override
    public void serializeValue(final Long value, final DataOutput out) throws IOException {

        out.writeLong(value);
    }

    @Override
    public Long deserializeValue(final DataInput in) throws IOException {

        return in.readLong();
    }

    @Override
    public String toString() {

        return NAME;
    }
}
