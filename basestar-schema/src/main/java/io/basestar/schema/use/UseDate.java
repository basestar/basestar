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

import io.basestar.schema.exception.InvalidTypeException;
import io.swagger.v3.oas.models.media.DateSchema;
import io.swagger.v3.oas.models.media.Schema;
import lombok.Data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Date;

@Data
public class UseDate implements UseScalar<LocalDate> {

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
    public LocalDate create(final Object value, final boolean expand, final boolean suppress) {

        if(value == null) {
            return null;
        } else if(value instanceof String) {
            return LocalDate.parse((String)value, DateTimeFormatter.ISO_LOCAL_DATE);
        } else if(value instanceof TemporalAccessor) {
            return LocalDate.from((TemporalAccessor) value);
        } else if(value instanceof Date) {
            return LocalDate.from(((Date) value).toInstant());
        } else if(suppress) {
            return null;
        } else {
            throw new InvalidTypeException();
        }
    }

    @Override
    public Code code() {

        return Code.DATE;
    }

    @Override
    public Object toJson() {

        return NAME;
    }

    @Override
    public void serializeValue(final LocalDate value, final DataOutput out) throws IOException {

        UseString.DEFAULT.serializeValue(value.toString(), out);
    }

    @Override
    public LocalDate deserializeValue(final DataInput in) throws IOException {

        return LocalDate.parse(UseString.DEFAULT.deserializeValue(in));
    }

    @Override
    public Schema<?> openApi() {

        return new DateSchema();
    }
}
