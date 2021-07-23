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

import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.Renaming;
import io.basestar.expression.constant.Constant;
import io.basestar.util.Name;
import io.basestar.util.Pair;
import lombok.Data;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * With
 *
 * Call the provided expression with additional bound variables
 */

@Data
public class With implements Expression {

    private static final String TOKEN = "WITH";

    public static final int PRECEDENCE = Case.PRECEDENCE + 1;

    private final List<Pair<String, Expression>> with;

    private final Expression yield;

    @Override
    public Expression bind(final Context context, final Renaming root) {

        boolean constant = true;
        boolean changed = false;
        final List<Pair<String, Expression>> with = new ArrayList<>();
        for(final Pair<String, Expression> entry : this.with) {
            final Expression before = entry.getSecond();
            final Expression after = before.bind(context, root);
            with.add(Pair.of(entry.getFirst(), after));
            constant = constant && after instanceof Constant;
            changed = changed || after != before;
        }
        if(constant) {
            final Expression _return = this.yield.bind(context, Renaming.closure(withKeys(), root));
            if(_return instanceof Constant) {
                return _return;
            } else {
                return new With(with, _return);
            }
        } else if(changed) {
            return new With(with, yield);
        } else {
            return this;
        }
    }

    @Override
    public Object evaluate(final Context context) {

        final Map<String, Object> with = new HashMap<>();
        for(final Pair<String, Expression> entry : this.with) {
            with.put(entry.getFirst(), entry.getSecond().evaluate(context));
        }
        return yield.evaluate(context.with(with));
    }

    @Override
    public Set<Name> names() {

        return Stream.concat(Stream.of(yield), withValues().stream())
                .flatMap(v -> v.names().stream())
                .collect(Collectors.toSet());
    }

    @Override
    public String token() {

        return TOKEN;
    }

    @Override
    public int precedence() {

        return PRECEDENCE;
    }

    private List<String> withKeys() {

        return with.stream().map(Pair::getFirst).collect(Collectors.toList());
    }

    private List<Expression> withValues() {

        return with.stream().map(Pair::getSecond).collect(Collectors.toList());
    }

    @Override
    public boolean isConstant(final Closure closure) {

        return yield.isConstant(closure.with(withKeys()));
    }

    @Override
    public <T> T visit(final ExpressionVisitor<T> visitor) {

        return visitor.visitWith(this);
    }

    @Override
    public List<Expression> expressions() {

        return Stream.concat(withValues().stream(), Stream.of(yield))
                .collect(Collectors.toList());
    }

    @Override
    public Expression copy(final List<Expression> expressions) {

        final List<Pair<String, Expression>> result = new ArrayList<>();
        int offset = 0;
        for(final Pair<String, Expression> entry : with) {
            result.add(Pair.of(entry.getFirst(), expressions.get(offset)));
            ++offset;
        }
        return new With(result, expressions.get(offset));
    }

    @Override
    public String toString() {

        return TOKEN + with.stream()
                .map(entry -> " " + entry.getFirst() + " AS (" + entry.getSecond() + ")")
                .collect(Collectors.joining(", ")) + " " + yield;
    }
}
