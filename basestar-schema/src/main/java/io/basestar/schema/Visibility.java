package io.basestar.schema;

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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.exception.SchemaSyntaxException;
import lombok.Data;

import java.io.Serializable;

public interface Visibility extends Serializable {

    @JsonCreator
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

    boolean apply(Context context);

    boolean isAlwaysVisible();

    boolean isAlwaysHidden();

    @Data
    class Constant implements Visibility {

        public static final Constant FALSE = new Constant(false);

        public static final Constant TRUE = new Constant(true);

        private final boolean value;

        @Override
        public Object toJson() {

            return true;
        }

        @Override
        public boolean apply(final Context context) {

            return value;
        }

        @Override
        public boolean isAlwaysVisible() {

            return value;
        }

        @Override
        public boolean isAlwaysHidden() {

            return !value;
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

        @Override
        public boolean apply(final Context context) {

            return expression.evaluatePredicate(context);
        }

        @Override
        public boolean isAlwaysVisible() {

            return false;
        }

        @Override
        public boolean isAlwaysHidden() {

            return false;
        }
    }
}
