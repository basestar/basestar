package io.basestar.expression;

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

import com.google.common.collect.ImmutableList;
import io.basestar.expression.constant.Constant;
import io.basestar.util.Name;

import java.util.List;
import java.util.Set;

public interface Unary extends Expression {

    Expression getOperand();

    Expression create(Expression operand);

    @Override
    default Set<Name> paths() {

        return getOperand().paths();
    }

    @Override
    default Expression bind(final Context context, final NameTransform root) {

        final Expression before = getOperand();
        final Expression after = before.bind(context, root);
        if(after instanceof Constant) {
            return new Constant(create(after).evaluate(context));
        } else if(before == after) {
            return this;
        } else {
            return create(after);
        }
    }

    default String toString(final Expression value) {

        final StringBuilder str = new StringBuilder();
        str.append(token());
        if(value.precedence() > precedence()) {
            str.append("(");
            str.append(value);
            str.append(")");
        } else {
            str.append(value);
        }
        return str.toString();
    }

    @Override
    default boolean isConstant(final Set<String> closure) {

        return getOperand().isConstant(closure);
    }

    @Override
    default List<Expression> expressions() {

        return ImmutableList.of(getOperand());
    }

    @Override
    default Expression copy(final List<Expression> expressions) {

        assert expressions.size() == 1;
        return create(expressions.get(0));
    }
}
