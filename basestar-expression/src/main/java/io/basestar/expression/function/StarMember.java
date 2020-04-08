package io.basestar.expression.function;

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

import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.PathTransform;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.constant.PathConstant;
import io.basestar.util.Path;
import lombok.Data;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Data
public class StarMember implements Expression {

    public static final String TOKEN = ".*.";

    public static final int PRECEDENCE = Member.PRECEDENCE;

    private final Expression with;

    private final String member;

    @Override
    public Expression bind(final Context context, final PathTransform root) {

        final Expression with = this.with.bind(context, root);
        // Fold x.y.z into a path constant, helps with query generation
        if(with instanceof Constant) {
            return new Constant(evaluate(context, ((Constant)with).getValue(), member));
        } else if(with instanceof PathConstant) {
            return new PathConstant(((PathConstant)with).getPath().with("*").with(member));
        } else if(with == this.with) {
            return this;
        } else {
            return new StarMember(with, member);
        }
    }

    @Override
    public Object evaluate(final Context context) {

        final Object with = this.with.evaluate(context);
        return evaluate(context, with, member);
    }

    public static Object evaluate(final Context context, final Object with, final String member) {

        if(with instanceof Map<?, ?>) {
            return ((Map<?, ?>) with).values().stream().map(
                    v -> context.member(v, member)
            ).collect(Collectors.toList());
        } else if(with instanceof Collection<?>) {
            return ((Collection<?>)with).stream().map(
                    v -> context.member(v, member)
            ).collect(Collectors.toList());
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public Set<Path> paths() {

        return with.paths();
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

        return visitor.visitStarMember(this);
    }

    @Override
    public String toString() {

        return with + TOKEN + member;
    }
}
