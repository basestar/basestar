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

import io.basestar.schema.exception.UnexpectedTypeException;
import io.basestar.util.Name;
import io.swagger.v3.oas.models.media.DateTimeSchema;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Date;
import java.util.Set;

public class UseDateTime implements UseScalar<LocalDateTime> {

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
    public LocalDateTime create(final Object value, final Set<Name> expand, final boolean suppress) {

        if(value instanceof String) {
            return LocalDateTime.parse((String)value, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        } else if(value instanceof TemporalAccessor) {
            return LocalDateTime.from((TemporalAccessor)value);
        } else if(value instanceof Date) {
            return ((Date) value).toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
        } else if(suppress) {
            return null;
        } else {
            throw new UnexpectedTypeException(this, value);
        }
    }

    @Override
    public Code code() {

        return Code.DATETIME;
    }

    @Override
    public Object toConfig() {

        return NAME;
    }

    @Override
    public void serializeValue(final LocalDateTime value, final DataOutput out) throws IOException {

        UseString.DEFAULT.serializeValue(value.toString(), out);
    }

    @Override
    public LocalDateTime deserializeValue(final DataInput in) throws IOException {

        return LocalDateTime.parse(UseString.DEFAULT.deserializeValue(in));
    }

    @Override
    public io.swagger.v3.oas.models.media.Schema<?> openApi() {

        return new DateTimeSchema();
    }

    @Override
    public String toString() {

        return NAME;
    }
}
