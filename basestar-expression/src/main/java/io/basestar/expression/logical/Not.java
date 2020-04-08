package io.basestar.expression.logical;

/*-
 * #%L
 * basestar-expression
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

import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.Unary;
import io.basestar.expression.arithmetic.Negate;
import lombok.Data;

/**
 * Not
 *
 * Logical Complement
 */

@Data
public class Not implements Unary {

    public static final String TOKEN = "!";

    public static final int PRECEDENCE = Negate.PRECEDENCE;

    private final Expression operand;

    /**
     * !operand
     *
     * @param operand any Operand
     */

    public Not(final Expression operand) {

        this.operand = operand;
    }

    @Override
    public Expression create(final Expression value) {

        return new Not(value);
    }

    @Override
    public Boolean evaluate(final Context context) {

        return !operand.evaluatePredicate(context);
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

        return visitor.visitNot(this);
    }

    @Override
    public String toString() {

        return Unary.super.toString(operand);
    }
}
