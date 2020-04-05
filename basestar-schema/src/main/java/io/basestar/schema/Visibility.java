package io.basestar.schema;

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
