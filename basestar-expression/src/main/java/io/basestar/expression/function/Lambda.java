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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.Renaming;
import io.basestar.expression.call.Callable;
import io.basestar.expression.constant.Constant;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import lombok.Data;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Lambda
 *
 */

@Data
public class Lambda implements Expression {

    public static final String TOKEN = "=>";

    public static final int PRECEDENCE = Constant.PRECEDENCE + 1;

    private final List<String> args;

    private final Expression yield;

    /**
     * (args...) -> yield
     *
     * @param args string
     * @param yield expression
     */

    public Lambda(final List<String> args, final Expression yield) {

        this.args = args;
        this.yield = yield;
    }

    @Override
    public Expression bind(final Context context, final Renaming root) {

        final BindContext bindContext = new BindContext(context);
        final Expression yield = this.yield.bind(bindContext, Renaming.closure(Immutable.set(args), root));

        if(yield == this.yield) {
            return this;
        } else {
            return new Lambda(args, yield);
        }
    }

    @Override
    public Object evaluate(final Context context) {

        // FIXME closure?
        return new Callable() {
            @Override
            public Object call(final Object... args) {

                final Map<String, Object> with = new HashMap<>();
                final int argC = Math.min(args.length, Lambda.this.args.size());
                for(int i = 0; i != argC; ++i) {
                    with.put(Lambda.this.args.get(i), args[i]);
                }
                return yield.evaluate(context.with(with));
            }

            @Override
            public Type type() {

                // FIXME
                return Object.class;
            }

            @Override
            public Type[] args() {

                // FIXME
                return args.stream().map(v -> Object.class).toArray(Type[]::new);
            }
        };
    }

    @Override
    public Set<Name> names() {

        return yield.names().stream()
                .filter(v -> !args.contains(v.first()))
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

        return yield.isConstant(closure.with(Immutable.set(args)));
    }

    @Override
    public <T> T visit(final ExpressionVisitor<T> visitor) {

        return visitor.visitLambda(this);
    }

    @Override
    public List<Expression> expressions() {

        return ImmutableList.of(yield);
    }

    @Override
    public Expression copy(final List<Expression> expressions) {

        assert expressions.size() == 1;
        return new Lambda(args, expressions.get(0));
    }

    @Override
    public String toString() {

        final String args = this.args.size() == 1 ? this.args.get(0) : "(" + Joiner.on(", ").join(this.args) + ")";
        return args + " " + TOKEN + " " + yield;
    }

    @Data
    private class BindContext implements Context {

        private final Context context;

        boolean fullyBound = true;

        @Override
        public Object get(final String name) {

            if(args.contains(name)) {
                throw new IllegalStateException("illegal bind to lambda parameter");
            } else {
                return context.get(name);
            }
        }

        @Override
        public boolean has(final String name) {

            final boolean arg = args.contains(name);
            final boolean has = !arg && context.has(name);
            fullyBound = fullyBound && (has || arg);
            return has;
        }

        @Override
        public Callable callable(final Type target, final String method, final Type... args) {

            return context.callable(target, method, args);
        }
    }
}
