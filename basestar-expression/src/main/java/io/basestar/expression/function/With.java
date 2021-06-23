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
import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * With
 *
 * Call the provided expression with additional bound variables
 */

@Data
public class With implements Expression {

    private static final String TOKEN = "with";

    public static final int PRECEDENCE = Index.PRECEDENCE + 1;

    private final Map<String, Expression> with;

    private final Expression yield;

    @Override
    public Expression bind(final Context context, final Renaming root) {

        boolean constant = true;
        boolean changed = false;
        final Map<String, Expression> with = new HashMap<>();
        for(final Map.Entry<String, Expression> entry : this.with.entrySet()) {
            final Expression before = entry.getValue();
            final Expression after = before.bind(context, root);
            with.put(entry.getKey(), after);
            constant = constant && after instanceof Constant;
            changed = changed || after != before;
        }
        if(constant) {
            final Expression _return = this.yield.bind(context, Renaming.closure(with.keySet(), root));
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
        for(final Map.Entry<String, Expression> entry : this.with.entrySet()) {
            with.put(entry.getKey(), entry.getValue().evaluate(context));
        }
        return yield.evaluate(context.with(with));
    }

    @Override
    public Set<Name> names() {

        return Stream.concat(Stream.of(yield), with.values().stream())
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

    @Override
    public boolean isConstant(final Closure closure) {

        return yield.isConstant(closure.with(with.keySet()));
    }

    @Override
    public <T> T visit(final ExpressionVisitor<T> visitor) {

        return visitor.visitWith(this);
    }

    @Override
    public List<Expression> expressions() {

        return Stream.concat(with.values().stream(), Stream.of(yield))
                .collect(Collectors.toList());
    }

    @Override
    public Expression copy(final List<Expression> expressions) {

        final Map<String, Expression> result = new HashMap<>();
        int offset = 0;
        for(final Map.Entry<String, Expression> entry : with.entrySet()) {
            result.put(entry.getKey(), expressions.get(offset));
            ++offset;
        }
        return new With(result, expressions.get(offset));
    }

    @Override
    public String toString() {

        return TOKEN + "(" + with.entrySet().stream()
                .map(entry -> entry.getKey() + " = " + entry.getValue())
                .collect(Collectors.joining(", ")) + ") " + yield;
    }
}
