package io.basestar.expression.bitwise;

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
import io.basestar.expression.arithmetic.Add;
import io.basestar.expression.type.match.BinaryMatch;
import io.basestar.expression.type.match.BinaryNumberMatch;
import lombok.Data;

/**
 * Bitwise Left-Shift
 */

@Data
public class BitLsh implements Binary {

    public static final String TOKEN = "<<";

    public static final int PRECEDENCE = Add.PRECEDENCE + 1;

    private final Expression lhs;

    private final Expression rhs;

    /**
     * lhs << rhs
     *
     * @param lhs integer Left hand operand
     * @param rhs integer Right hand operand
     */

    public BitLsh(final Expression lhs, final Expression rhs) {

        this.lhs = lhs;
        this.rhs = rhs;
    }

    @Override
    public Expression create(final Expression lhs, final Expression rhs) {

        return new BitLsh(lhs, rhs);
    }

    @Override
    public Long evaluate(final Context context) {

        return VISITOR.apply(lhs.evaluate(context), rhs.evaluate(context));
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

        return visitor.visitBitLsh(this);
    }

    @Override
    public String toString() {

        return Binary.super.toString(lhs, rhs);
    }

    private static final BinaryNumberMatch<Long> NUMBER_VISITOR = new BinaryNumberMatch.Promoting<Long>() {

        @Override
        public String toString() {

            return TOKEN;
        }

        @Override
        public Long apply(final Long lhs, final Long rhs) {

            return lhs << rhs;
        }
    };

    private static final BinaryMatch<Long> VISITOR = new BinaryMatch.Coercing<Long>() {

        @Override
        public String toString() {

            return TOKEN;
        }

        @Override
        public Long apply(final Number lhs, final Number rhs) {

            return NUMBER_VISITOR.apply(lhs, rhs);
        }
    };
}
