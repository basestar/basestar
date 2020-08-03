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
import io.swagger.v3.oas.models.media.NumberSchema;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

/**
 * Number Type
 *
 * Stored as double-precision floating point.
 *
 * <strong>Example</strong>
 * <pre>
 * type: number
 * </pre>
 */

@Data
@Slf4j
public class UseNumber implements UseScalar<Double> {

    public static final UseNumber DEFAULT = new UseNumber();

    public static final String NAME = "number";

    public static UseNumber from(final Object config) {

        return DEFAULT;
    }

    @Override
    public <R> R visit(final Visitor<R> visitor) {

        return visitor.visitNumber(this);
    }

    @Override
    public Object toConfig() {

        return NAME;
    }

    @Override
    public Double create(final Object value, final Set<Name> expand, final boolean suppress) {

        if(value instanceof Boolean) {
            return ((Boolean)value) ? 1.0 : 0.0;
        } else if(value instanceof Number) {
            return ((Number)value).doubleValue();
        } else if(value instanceof String) {
            try {
                return Double.parseDouble((String)value);
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

        return Code.NUMBER;
    }

    @Override
    public io.swagger.v3.oas.models.media.Schema<?> openApi() {

        return new NumberSchema();
    }

    @Override
    public void serializeValue(final Double value, final DataOutput out) throws IOException {

        out.writeDouble(value);
    }

    @Override
    public Double deserializeValue(final DataInput in) throws IOException {

        return in.readDouble();
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
