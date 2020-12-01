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
import io.basestar.expression.type.Values;
import io.basestar.expression.type.match.BinaryMatch;
import io.basestar.expression.type.match.BinaryNumberMatch;
import lombok.Data;

import java.lang.reflect.Type;

/**
 * Pow
 *
 * Numeric exponentiation
 */

@Data
public class Pow implements Binary {

    public static final String TOKEN = "**";

    public static final int PRECEDENCE = Negate.PRECEDENCE + 1;

    private final Expression lhs;

    private final Expression rhs;

    /**
     * lhs ** rhs
     *
     * @param lhs number Left hand operand
     * @param rhs number Right hand operand
     */

    public Pow(final Expression lhs, final Expression rhs) {

        this.lhs = lhs;
        this.rhs = rhs;
    }

    @Override
    public Expression create(final Expression lhs, final Expression rhs) {

        return new Pow(lhs, rhs);
    }

    @Override
    public Number evaluate(final Context context) {

        return VISITOR.apply(lhs.evaluate(context), rhs.evaluate(context));
    }

    @Override
    public Type type(final Context context) {

        return VISITOR.apply(Values.defaultValue(lhs.type(context)), Values.defaultValue(rhs.type(context))).getClass();
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

        return visitor.visitPow(this);
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

            return (long) Math.pow(lhs, rhs);
        }

        @Override
        public Number apply(final Double lhs, final Double rhs) {

            return Math.pow(lhs, rhs);
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
