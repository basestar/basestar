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
import io.swagger.v3.oas.models.media.IntegerSchema;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
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
public class UseInteger implements UseScalar<Long> {

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
    public Object toConfig() {

        return NAME;
    }

    @Override
    public Long create(final Object value, final Set<Name> expand, final boolean suppress) {

        if(value instanceof Boolean) {
            return ((Boolean)value) ? 1L : 0L;
        } else if(value instanceof Number) {
            return ((Number)value).longValue();
        } else if(value instanceof String) {
            try {
                return Long.parseLong((String) value);
            } catch (final NumberFormatException e) {
                if(suppress) {
                    log.warn("Suppressed conversion error", e);
                    return null;
                } else {
                    throw e;
                }
            }
        } else if(suppress) {
            log.warn("Suppressed conversion error (invalid type: " + value.getClass() + ")");
            return null;
        } else {
            throw new UnexpectedTypeException(this, value);
        }
    }

    @Override
    public Code code() {

        return Code.INTEGER;
    }

    @Override
    public io.swagger.v3.oas.models.media.Schema<?> openApi() {

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

//    @Override
//    public Map<String, Object> openApiType() {
//
//        return ImmutableMap.of(
//                "type", "number"
//        );
//    }
}
