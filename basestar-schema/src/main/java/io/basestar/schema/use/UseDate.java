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

import io.basestar.exception.InvalidDateTimeException;
import io.basestar.expression.type.exception.TypeConversionException;
import io.basestar.util.ISO8601;
import io.basestar.util.Name;
import io.swagger.v3.oas.models.media.DateSchema;
import io.swagger.v3.oas.models.media.Schema;
import lombok.Data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Type;
import java.time.LocalDate;
import java.util.Objects;
import java.util.Set;

@Data
public class UseDate implements UseStringLike<LocalDate> {

    public static final UseDate DEFAULT = new UseDate();

    public static final String NAME = "date";

    @Override
    public <R> R visit(final Visitor<R> visitor) {

        return visitor.visitDate(this);
    }

    public static UseDate from(final Object config) {

        return DEFAULT;
    }

    @Override
    public LocalDate create(final Object value, final Set<Name> expand, final boolean suppress) {

        try {
            return ISO8601.toDate(value);
        } catch (final InvalidDateTimeException e) {
            if(suppress) {
                return null;
            } else {
                throw new TypeConversionException(LocalDate.class, value);
            }
        }
    }

    @Override
    public Code code() {

        return Code.DATE;
    }

    @Override
    public Type javaType(final Name name) {

        return LocalDate.class;
    }

    @Override
    public LocalDate defaultValue() {

        return LocalDate.ofEpochDay(0);
    }

    @Override
    public Object toConfig(final boolean optional) {

        return Use.name(NAME, optional);
    }

    @Override
    public void serializeValue(final LocalDate value, final DataOutput out) throws IOException {

        UseString.DEFAULT.serializeValue(ISO8601.toString(value), out);
    }

    @Override
    public LocalDate deserializeValue(final DataInput in) throws IOException {

        return ISO8601.parseDate(UseString.DEFAULT.deserializeValue(in));
    }

    @Override
    public Schema<?> openApi(final Set<Name> expand) {

        return new DateSchema();
    }

    @Override
    public String toString() {

        return NAME;
    }

    @Override
    public String toString(final LocalDate value) {

        if(value == null) {
            return "null";
        } else {
            return ISO8601.toString(value);
        }
    }

    @Override
    public boolean areEqual(final LocalDate a, final LocalDate b) {

        return Objects.equals(a, b);
    }
}
