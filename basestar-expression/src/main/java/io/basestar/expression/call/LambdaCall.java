package io.basestar.expression.call;

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
import io.basestar.expression.constant.Constant;
import io.basestar.expression.constant.NameConstant;
import io.basestar.expression.function.Member;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import lombok.Data;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Call
 */

@Data
public class LambdaCall implements Expression {

    public static final String TOKEN = "()";

    public static final int PRECEDENCE = Member.PRECEDENCE;

    private final Expression with;

    private final List<Expression> args;

    /**
     * with(args...)
     *
     * @param with any Lambda callable
     * @param args args Function arguments
     */

    public LambdaCall(final Expression with, final List<Expression> args) {

        this.with = with;
        this.args = Immutable.list(args);
    }

    @Override
    public Expression bind(final Context context, final Renaming root) {

        final Expression with = this.with.bind(context, root);
        final List<Expression> args = new ArrayList<>();
        boolean constant = with instanceof Constant;
        boolean changed = with != this.with;
        for(final Expression before : this.args) {
            final Expression after = before.bind(context, root);
            args.add(after);
            constant = constant && after instanceof Constant;
            changed = changed || (before != after);
        }
        if(constant) {
            return new Constant(new LambdaCall(with, args).evaluate(context));
        } else if(with == this.with) {
            return this;
        } else {
            return new LambdaCall(with, args);
        }
    }

    @Override
    public Object evaluate(final Context context) {

        if(this.with instanceof NameConstant) {
            final Name name = ((NameConstant)this.with).getName();
            final Object[] args = this.args.stream().map(v -> v.evaluate(context)).toArray();
            final Type[] argTypes = Arrays.stream(args).map(Object::getClass).toArray(Type[]::new);
            return context.callable(name.toString(), argTypes).call(args);
        } else {
            final Object with = this.with.evaluate(context);
            final Object[] args = this.args.stream().map(v -> v.evaluate(context)).toArray();
            if (with instanceof Callable) {
                return ((Callable) with).call(args);
            } else {
                throw new IllegalStateException();
            }
        }
    }

    @Override
    public Set<Name> names() {

        return Stream.concat(Stream.of(with), args.stream())
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

        return this.with.isConstant(closure) && this.args.stream().allMatch(v -> v.isConstant(closure));
    }

    @Override
    public <T> T visit(final ExpressionVisitor<T> visitor) {

        return visitor.visitLambdaCall(this);
    }

    @Override
    public List<Expression> expressions() {

        return ImmutableList.<Expression>builder()
                .add(with)
                .addAll(args)
                .build();
    }

    @Override
    public Expression copy(final List<Expression> expressions) {

        assert expressions.size() > 0;
        return new LambdaCall(expressions.get(0), expressions.subList(1, expressions.size()));
    }

    @Override
    public String toString() {

        final String with = this.with.precedence() > precedence() ? "(" + this.with + ")" : this.with.toString();
        final String args = "(" + Joiner.on(", ").join(this.args) + ")";
        return with + args;
    }
}
