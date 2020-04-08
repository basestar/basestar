package io.basestar.schema;

/*-
 * #%L
 * basestar-schema
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2020 basestar.io
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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.basestar.expression.Expression;
import io.basestar.schema.exception.SchemaSyntaxException;
import lombok.Data;

import java.io.Serializable;

public interface Visibility extends Serializable {

    @JsonCreator
    @SuppressWarnings("unchecked")
    static Visibility from(final Object value) {

        if(value instanceof Boolean) {
            return (Boolean)value ? Constant.TRUE : Constant.FALSE;
        } else if(value instanceof String) {
            return new Dynamic((String)value);
        } else {
            throw new SchemaSyntaxException("Invalid visibility value");
        }
    }

    @JsonValue
    Object toJson();

    @Data
    class Constant implements Visibility {

        public static final Constant FALSE = new Constant(false);

        public static final Constant TRUE = new Constant(true);

        private final boolean value;

        @Override
        public Object toJson() {

            return true;
        }
    }

    @Data
    class Dynamic implements Visibility {

        private final Expression expression;

        public Dynamic(final String expression) {

            this.expression = Expression.parse(expression);
        }

        @Override
        public Object toJson() {

            return expression.toString();
        }
    }
}
