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
import io.basestar.util.ISO8601;
import io.basestar.util.Name;
import io.swagger.v3.oas.models.media.DateTimeSchema;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Type;
import java.time.Instant;
import java.util.Objects;
import java.util.Set;

public class UseDateTime implements UseStringLike<Instant> {

    public static final UseDateTime DEFAULT = new UseDateTime();

    public static final String NAME = "datetime";

    @Override
    public <R> R visit(final Visitor<R> visitor) {

        return visitor.visitDateTime(this);
    }

    public static UseDateTime from(final Object config) {

        return DEFAULT;
    }

    @Override
    public Instant create(final ValueContext context, final Object value, final Set<Name> expand) {

        return context.createDateTime(this, value, expand);
    }

    @Override
    public Code code() {

        return Code.DATETIME;
    }

    @Override
    public Type javaType(final Name name) {

        return Instant.class;
    }

    @Override
    public Instant defaultValue() {

        return Instant.ofEpochMilli(0);
    }

    @Override
    public Object toConfig(final boolean optional) {

        return Use.name(NAME, optional);
    }

    @Override
    public void serializeValue(final Instant value, final DataOutput out) throws IOException {

        UseString.DEFAULT.serializeValue(ISO8601.toString(value), out);
    }

    @Override
    public Instant deserializeValue(final DataInput in) throws IOException {

        return ISO8601.parseDateTime(UseString.DEFAULT.deserializeValue(in));
    }

    @Override
    public io.swagger.v3.oas.models.media.Schema<?> openApi(final Set<Name> expand) {

        return new DateTimeSchema();
    }

    @Override
    public String toString() {

        return NAME;
    }

    @Override
    public String toString(final Instant value) {

        if(value == null) {
            return "null";
        } else {
            return ISO8601.toString(value);
        }
    }

    @Override
    public boolean areEqual(final Instant a, final Instant b) {

        return Objects.equals(a, b);
    }
}
