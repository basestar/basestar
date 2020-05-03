package io.basestar.expression.constant;

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

import com.google.common.collect.ImmutableSet;
import io.basestar.expression.*;
import io.basestar.expression.exception.UndefinedNameException;
import io.basestar.util.Path;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Iterator;
import java.util.Set;
import java.util.function.Function;

@Data
@AllArgsConstructor
public class PathConstant implements Expression {

    public static final String TOKEN = ".";

    public static final int PRECEDENCE = 0;

    private final Path path;

    public PathConstant(final String name) {

        this.path = new Path(name);
    }

    @Override
    public Expression bind(final Context context, final PathTransform root) {

        if (context.has(path.first())) {
            final Object target = context.get(path.first());
            return new Constant(resolve(target, path.withoutFirst(), context));
        } else {
            final Path newPath = root.transform(path);
            if(newPath == path) {
                return this;
            } else {
                return new PathConstant(newPath);
            }
        }
    }

    @Override
    public Object evaluate(final Context context) {

        if(context.has(path.first())) {
            final Object target = context.get(path.first());
            return resolve(target, path.withoutFirst(), context);
        } else {
            throw new UndefinedNameException(path.first());
        }
    }

    private Object resolve(Object target, final Path tail, final Context context) {

        final Iterator<String> iter = tail.iterator();
        while(iter.hasNext()) {
            final String part = iter.next();
            target = context.member(target, part);
        }
        return target;
    }

    @Override
    public Set<Path> paths() {

        return ImmutableSet.of(path);
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

        return closure.stream().anyMatch(c -> path.isChildOrEqual(Path.of(c)));
    }

    @Override
    public <T> T visit(final ExpressionVisitor<T> visitor) {

        return visitor.visitPathConstant(this);
    }

    @Override
    public String toString() {

        return path.toString();
    }

    public static Matcher<PathConstant> match() {

        return match(p -> p);
    }

    public static <R> Matcher<R> match(final Function<PathConstant, R> then) {

        return e -> {
            if(e instanceof PathConstant) {
                return then.apply((PathConstant) e);
            } else {
                return null;
            }
        };
    }
}
