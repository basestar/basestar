package io.basestar.expression.iterate;

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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.PathTransform;
import io.basestar.expression.function.IfElse;
import io.basestar.util.Path;
import lombok.Data;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

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
    public Expression bind(final Context context, final PathTransform root) {

        final PathTransform closureTransform = PathTransform.closure(iter.closure(), root);
        final Expression yieldKey = this.yieldKey.bind(context, closureTransform);
        final Expression yieldValue = this.yieldValue.bind(context, closureTransform);
        final Expression iter = this.iter.bind(context, root);
        if(yieldKey == this.yieldKey && yieldValue == this.yieldValue && iter == this.iter) {
            return this;
        } else {
            return new ForObject(yieldKey, yieldValue, iter);
        }
    }

    @Override
    public Map<String, ?> evaluate(final Context context) {

        final Object iter = this.iter.evaluate(context);
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
    public Set<Path> paths() {

        return ImmutableSet.<Path>builder()
                .addAll(yieldKey.paths())
                .addAll(yieldValue.paths())
                .addAll(iter.paths())
                .build();
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

        return visitor.visitForObject(this);
    }

    @Override
    public String toString() {

        return "{" + yieldKey + ": " + yieldValue + " " + TOKEN + " " + iter + "}";
    }
}
