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
import com.google.common.collect.Streams;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.PathTransform;
import io.basestar.expression.constant.Constant;
import io.basestar.util.Path;
import lombok.Data;

import java.util.*;

/**
 * Set Comprehension
 *
 * Create a set from an iterator
 *
 * @see io.basestar.expression.iterate.Of
 */

@Data
public class ForSet implements Expression {

    public static final String TOKEN = "for";

    public static final int PRECEDENCE = ForObject.PRECEDENCE + 1;

    private final Expression yield;

    private final Expression iter;

    /**
     * [yield for iter]
     *
     * @param yield expression Value-yielding expression
     * @param iter iterator Iterator
     */

    public ForSet(final Expression yield, final Expression iter) {

        this.yield = yield;
        this.iter = iter;
    }

    @Override
    public Expression bind(final Context context, final PathTransform root) {

        final Set<String> closure = iter.closure();
        final Expression yield = this.yield.bind(context, PathTransform.closure(closure, root));
        final Expression iter = this.iter.bind(context, root);
        if(iter instanceof Constant && yield.isConstant(closure)) {
            return new Constant(evaluate(context, yield, ((Constant) iter).getValue()));
        } else if(yield == this.yield && iter == this.iter) {
            return this;
        } else {
            return new ForSet(yield, iter);
        }
    }

    @Override
    public Set<?> evaluate(final Context context) {

        return evaluate(context, yield, iter.evaluate(context));
    }

    private Set<?> evaluate(final Context context, final Expression yield, final Object iter) {

        if(iter instanceof Iterator<?>) {
            final Set<Object> result = new HashSet<>();
            Streams.stream((Iterator<?>)iter)
                    .forEach(v -> {
                        @SuppressWarnings("unchecked")
                        final Map<String, Object> scope = (Map<String, Object>)v;
                        final Object value = yield.evaluate(context.with(scope));
                        result.add(value);
                    });
            return result;
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public Set<Path> paths() {

        return ImmutableSet.<Path>builder()
                .addAll(yield.paths())
                .addAll(iter.paths())
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
            return yield.isConstant(fullClosure);
        } else {
            return false;
        }
    }

    @Override
    public <T> T visit(final ExpressionVisitor<T> visitor) {

        return visitor.visitForSet(this);
    }

    @Override
    public List<Expression> expressions() {

        return ImmutableList.of(yield, iter);
    }

    @Override
    public Expression copy(final List<Expression> expressions) {

        assert expressions.size() == 2;
        return new ForSet(expressions.get(0), expressions.get(1));
    }

    @Override
    public String toString() {

        return "{" + yield + " " + TOKEN + " " + iter + "}";
    }
}
