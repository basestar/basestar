package io.basestar.expression.iterate;

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
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.Renaming;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.literal.LiteralObject;
import io.basestar.util.Name;
import lombok.Data;

import java.util.*;

/**
 * Iterate
 *
 * Create an iterator
 *
 * @see io.basestar.expression.iterate.Where
 */

@Data
public class Of implements Expression {

    public static final String TOKEN = "of";

    public static final int PRECEDENCE = LiteralObject.PRECEDENCE + 1;

    private final String key;

    private final String value;

    private final Expression expr;


    /**
     * key:value of expr
     *
     * @param key identifier Name-binding for key
     * @param value identifier Name-binding for value
     * @param expr map Map to iterate
     */

    public Of(final String key, final String value, final Expression expr) {

        this.key = key;
        this.value = value;
        this.expr = expr;
    }

    /**
     * value of expr
     *
     * @param value identifier Name-binding for value
     * @param expr collection Collection to iterate
     */

    public Of(final String value, final Expression expr) {

        this(null, value, expr);
    }

    @Override
    public Expression bind(final Context context, final Renaming root) {

        final Expression expr = this.expr.bind(context, root);
        if(expr instanceof Constant) {
            return new Constant(evaluate(((Constant) expr).getValue()));
        } else if(expr == this.expr) {
            return this;
        } else {
            return new Of(key, value, expr);
        }
    }

    @Override
    public Set<String> closure() {

        if(key != null) {
            return ImmutableSet.of(key, value);
        } else {
            return ImmutableSet.of(value);
        }
    }

    @Override
    public Iterator<?> evaluate(final Context context) {

        final Object with = this.expr.evaluate(context);
        return evaluate(with);
    }

    private Iterator<?> evaluate(final Object with) {

        if(with instanceof Collection<?>) {
            if(key == null) {
                return ((Collection<?>) with).stream()
                        .map(v -> Collections.singletonMap(value, v))
                        .iterator();
            } else {
                throw new IllegalStateException();
            }
        } else if(with instanceof Map<?, ?>) {
            if(key != null) {
                return ((Map<?, ?>)with).entrySet().stream()
                        .map(v -> {
                            final Map<String, Object> entry = new HashMap<>();
                            entry.put(key, v.getKey());
                            entry.put(value, v.getValue());
                            return entry;
                        })
                        .iterator();
            } else {
                return ((Map<?, ?>)with).values().stream()
                        .map(o -> Collections.singletonMap(value, o))
                        .iterator();
            }
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public Set<Name> names() {

        return expr.names();
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

        return expr.isConstant(closure);
    }

    @Override
    public <T> T visit(final ExpressionVisitor<T> visitor) {

        return visitor.visitOf(this);
    }

    @Override
    public List<Expression> expressions() {

        return ImmutableList.of(expr);
    }

    @Override
    public Expression copy(final List<Expression> expressions) {

        assert expressions.size() == 1;
        return new Of(key, value, expressions.get(0));
    }

    @Override
    public String toString() {

        final String as = key == null ? value : "(" + key + "," + value + ")";
        return as + " " + TOKEN + " " + expr;
    }
}
