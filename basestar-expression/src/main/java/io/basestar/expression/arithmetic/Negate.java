package io.basestar.expression.arithmetic;

/*-
 * #%L
 * basestar-expression
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

import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.Unary;
import io.basestar.expression.function.Index;
import io.basestar.expression.type.match.UnaryMatch;
import io.basestar.expression.type.match.UnaryNumberMatch;
import lombok.Data;

import java.lang.reflect.Type;

/**
 * Negate
 */


@Data
public class Negate implements Unary {

    public static final String TOKEN = "-";

    public static final int PRECEDENCE = Index.PRECEDENCE + 1;

    private final Expression operand;

    /**
     * -operand
     *
     * @param operand number Operand
     */

    public Negate(final Expression operand) {

        this.operand = operand;
    }

    @Override
    public Expression create(final Expression with) {

        return new Negate(with);
    }

    @Override
    public Number evaluate(final Context context) {

        return VISITOR.apply(operand.evaluate(context));
    }

    @Override
    public Type type(final Context context) {

        return operand.type(context);
    }

//    @Override
//    public Query query() {
//
//        return Query.and();
//    }

    @Override
    public String token() {

        return TOKEN;
    }

    @Override
    public int precedence() {

        return PRECEDENCE;
    }

    @Override
    public <T> T visit(final ExpressionVisitor<T> visitor) {

        return visitor.visitNegate(this);
    }

    @Override
    public String toString() {

        return Unary.super.toString(operand);
    }

    private static final UnaryNumberMatch<Number> NUMBER_VISITOR = new UnaryNumberMatch<Number>() {

        @Override
        public String toString() {

            return TOKEN;
        }

        @Override
        public Number apply(final Long value) {

            return -value;
        }

        @Override
        public Number apply(final Double value) {

            return -value;
        }
    };

    private static final UnaryMatch<Number> VISITOR = new UnaryMatch<Number>() {

        @Override
        public String toString() {

            return TOKEN;
        }

        @Override
        public Number apply(final Number value) {

            return NUMBER_VISITOR.apply(value);
        }
    };
}
