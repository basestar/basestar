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
import com.google.common.collect.Sets;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.Renaming;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.function.IfElse;
import io.basestar.util.Name;
import io.basestar.util.Streams;
import lombok.Data;

import java.lang.reflect.Type;
import java.util.*;

/**
 * Object Comprehension
 *
 * Create an object from an iterator
 *
 * @see io.basestar.expression.iterate.Of
 */

@Data
public class ForObject implements Expression {

    public static final String TOKEN = "for";

    public static final int PRECEDENCE = IfElse.PRECEDENCE + 1;

    private final Expression yieldKey;

    private final Expression yieldValue;

    private final Expression iter;

    /**
     * {yieldKey:yieldValue for iter}
     *
     * @param yieldKey expression Key-yielding expression
     * @param yieldValue expression Value-yielding expression
     * @param iter iterator Iterator
     */

    public ForObject(final Expression yieldKey, final Expression yieldValue, final Expression iter) {

        this.yieldKey = yieldKey;
        this.yieldValue = yieldValue;
        this.iter = iter;
    }

    @Override
    public Expression bind(final Context context, final Renaming root) {

        final Set<String> closure = iter.closure();
        final Renaming closureTransform = Renaming.closure(closure, root);
        final Expression yieldKey = this.yieldKey.bind(context, closureTransform);
        final Expression yieldValue = this.yieldValue.bind(context, closureTransform);
        final Expression iter = this.iter.bind(context, root);
        if(iter instanceof Constant && yieldKey.isConstant(closure) && yieldValue.isConstant(closure)) {
            return new Constant(evaluate(context, yieldKey, yieldValue, ((Constant) iter).getValue()));
        } else if(yieldKey == this.yieldKey && yieldValue == this.yieldValue && iter == this.iter) {
            return this;
        } else {
            return new ForObject(yieldKey, yieldValue, iter);
        }
    }

    @Override
    public Map<String, ?> evaluate(final Context context) {

        return evaluate(context, yieldKey, yieldValue, iter.evaluate(context));
    }

    private Map<String, ?> evaluate(final Context context, final Expression yieldKey, final Expression yieldValue, final Object iter) {

        if(iter instanceof Iterator<?>) {
            final Map<String, Object> result = new HashMap<>();
            Streams.stream((Iterator<?>)iter)
                    .forEach(v -> {
                        @SuppressWarnings("unchecked")
                        final Map<String, Object> scope = (Map<String, Object>)v;
                        final Object key = yieldKey.evaluate(context.with(scope));
                        final Object value = yieldValue.evaluate(context.with(scope));
                        result.put(key.toString(), value);
                    });
            return result;
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public Type type(final Context context) {

        return Map.class;
    }

    @Override
    public Set<Name> names() {

        return ImmutableSet.<Name>builder()
                .addAll(yieldKey.names())
                .addAll(yieldValue.names())
                .addAll(iter.names())
                .build();
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
    public boolean isConstant(final Set<String> closure) {

        if(iter.isConstant(closure)) {
            final Set<String> fullClosure = Sets.union(closure, iter.closure());
            return yieldKey.isConstant(fullClosure) && yieldValue.isConstant(fullClosure);
        } else {
            return false;
        }
    }

    @Override
    public <T> T visit(final ExpressionVisitor<T> visitor) {

        return visitor.visitForObject(this);
    }

    @Override
    public List<Expression> expressions() {

        return ImmutableList.of(yieldKey, yieldValue, iter);
    }

    @Override
    public Expression copy(final List<Expression> expressions) {

        assert expressions.size() == 3;
        return new ForObject(expressions.get(0), expressions.get(1), expressions.get(2));
    }

    @Override
    public String toString() {

        return "{" + yieldKey + ": " + yieldValue + " " + TOKEN + " " + iter + "}";
    }
}
