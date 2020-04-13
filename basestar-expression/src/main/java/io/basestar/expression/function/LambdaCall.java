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
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.PathTransform;
import io.basestar.expression.constant.Constant;
import io.basestar.util.Path;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Data
public class LambdaCall implements Expression {

    public static final String TOKEN = "()";

    public static final int PRECEDENCE = MemberCall.PRECEDENCE;

    private final Expression with;

    private final List<Expression> args;

    @Override
    public Expression bind(final Context context, final PathTransform root) {

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

        final Object with = this.with.evaluate(context);
        if(with instanceof Lambda.Callable) {
            final Object[] args = this.args.stream().map(v -> v.evaluate(context)).toArray();
            return ((Lambda.Callable)with).call(args);
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public Set<Path> paths() {

        return Stream.concat(Stream.of(with), args.stream())
                .flatMap(v -> v.paths().stream())
                .collect(Collectors.toSet());
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
    public boolean isConstant(final Set<String> closure) {

        return false;
    }

    @Override
    public <T> T visit(final ExpressionVisitor<T> visitor) {

        return visitor.visitLambdaCall(this);
    }

    @Override
    public String toString() {

        final String with = this.with.precedence() > precedence() ? "(" + this.with + ")" : this.with.toString();
        return with + "(" + Joiner.on(", ").join(args) + ")";
    }
}
