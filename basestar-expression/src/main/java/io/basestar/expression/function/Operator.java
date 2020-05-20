package io.basestar.expression.function;

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
import lombok.Data;

@Data
public class Operator implements Binary {

    public static final int PRECEDENCE = Coalesce.PRECEDENCE + 1;

    private final String name;

    private final Expression lhs;

    private final Expression rhs;

    @Override
    public Expression create(final Expression lhs, final Expression rhs) {

        return new Operator(name, lhs, rhs);
    }

    @Override
    public Object evaluate(final Context context) {

        // FIXME:
        throw new UnsupportedOperationException();
    }

    @Override
    public String token() {

        return name;
    }

    @Override
    public int precedence() {

        return PRECEDENCE;
    }

    @Override
    public <T> T visit(final ExpressionVisitor<T> visitor) {

        return visitor.visitOperator(this);
    }

    @Override
    public String toString() {

        return lhs + " " +  name + " " + rhs;
    }
}
