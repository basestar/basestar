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

import io.basestar.expression.Binary;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.match.BinaryMatch;
import io.basestar.expression.match.BinaryNumberMatch;
import lombok.Data;

/**
 * Subtract
 */

@Data
public class Sub implements Binary {

    public static final String TOKEN = "-";

    public static final int PRECEDENCE = Add.PRECEDENCE;

    private final Expression lhs;

    private final Expression rhs;

    /**
     * lhs - rhs
     *
     * @param lhs number Left hand operand
     * @param rhs number Right hand operand
     */

    public Sub(final Expression lhs, final Expression rhs) {

        this.lhs = lhs;
        this.rhs = rhs;
    }

    @Override
    public Expression create(final Expression lhs, final Expression rhs) {

        return new Sub(lhs, rhs);
    }

    @Override
    public Number evaluate(final Context context) {

        return apply(lhs.evaluate(context), rhs.evaluate(context));
    }

    public static Number apply(final Object lhs, final Object rhs) {

        return VISITOR.apply(lhs, rhs);
    }

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

        return visitor.visitSub(this);
    }

    @Override
    public String toString() {

        return Binary.super.toString(lhs, rhs);
    }

    private static final BinaryNumberMatch<Number> NUMBER_VISITOR = new BinaryNumberMatch.Promoting<Number>() {

        @Override
        public String toString() {

            return TOKEN;
        }

        @Override
        public Number apply(final Long lhs, final Long rhs) {

            return lhs - rhs;
        }

        @Override
        public Number apply(final Double lhs, final Double rhs) {

            return lhs - rhs;
        }
    };

    private static final BinaryMatch<Number> VISITOR = new BinaryMatch.Coercing<Number>() {

        @Override
        public String toString() {

            return TOKEN;
        }

        @Override
        public Number apply(final Number lhs, final Number rhs) {

            return NUMBER_VISITOR.apply(lhs, rhs);
        }
    };
}
