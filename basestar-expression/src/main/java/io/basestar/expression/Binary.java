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
import com.google.common.collect.ImmutableSet;
import io.basestar.expression.constant.Constant;
import io.basestar.util.Name;

import java.util.List;
import java.util.Set;

public interface Binary extends Expression {

    Expression getLhs();

    Expression getRhs();

    Expression create(Expression lhs, Expression rhs);

    @Override
    default Set<Name> paths() {

        return ImmutableSet.<Name>builder()
                .addAll(getLhs().paths())
                .addAll(getRhs().paths())
                .build();
    }

    default String toString(final Expression lhs, final Expression rhs) {

        final StringBuilder str = new StringBuilder();
        if(lhs.precedence() > precedence()) {
            str.append("(");
            str.append(lhs);
            str.append(")");
        } else {
            str.append(lhs);
        }
        str.append(" ");
        str.append(token());
        str.append(" ");
        if(rhs.precedence() > precedence()) {
            str.append("(");
            str.append(rhs);
            str.append(")");
        } else {
            str.append(rhs);
        }
        return str.toString();
    }

//    @Override
//    default String toString(final int precedence) {
//
//        return "(" + getLhs().toString(precedence) + token() + getRhs().toString(precedence) + ")";
//    }

    @Override
    default Expression bind(final Context context, final Renaming root) {

        final Expression beforeLhs = getLhs();
        final Expression beforeRhs = getRhs();
        final Expression afterLhs = bindLhs(context, root);
        final Expression afterRhs = bindRhs(context, root);
        if(afterLhs instanceof Constant && afterRhs instanceof Constant) {
            return new Constant(create(afterLhs, afterRhs).evaluate(context));
        } else {
            return beforeLhs == afterLhs && beforeRhs == afterRhs ? this : create(afterLhs, afterRhs);
        }
    }

    default Expression bindLhs(final Context context, final Renaming root) {

        return getLhs().bind(context, root);
    }

    default Expression bindRhs(final Context context, final Renaming root) {

        return getRhs().bind(context, root);
    }

    @Override
    default boolean isConstant(final Set<String> closure) {

        return getLhs().isConstant(closure) && getRhs().isConstant(closure);
    }

    @Override
    default List<Expression> expressions() {

        return ImmutableList.of(getLhs(), getRhs());
    }

    @Override
    default Expression copy(final List<Expression> expressions) {

        assert expressions.size() == 2;
        return create(expressions.get(0), expressions.get(1));
    }
}
