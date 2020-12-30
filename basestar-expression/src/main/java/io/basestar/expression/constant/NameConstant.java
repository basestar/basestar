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
import io.basestar.util.Name;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

@Data
@AllArgsConstructor
public class NameConstant implements Expression {

    public static final String TOKEN = ".";

    public static final int PRECEDENCE = 0;

    private final Name name;

    public NameConstant(final String name) {

        this.name = Name.of(name);
    }

    @Override
    public Expression bind(final Context context, final Renaming root) {

        if (context.has(name.first())) {
            final Object target = context.get(name.first());
            return new Constant(resolve(target, name.withoutFirst(), context));
        } else {
            final Name newName = root.apply(name);
            if(newName == name) {
                return this;
            } else {
                return new NameConstant(newName);
            }
        }
    }

    @Override
    public Object evaluate(final Context context) {

        if(context.has(name.first())) {
            final Object target = context.get(name.first());
            return resolve(target, name.withoutFirst(), context);
        } else {
            throw new UndefinedNameException(name.first());
        }
    }

    private Object resolve(Object target, final Name tail, final Context context) {

        final Iterator<String> iter = tail.iterator();
        while(iter.hasNext()) {
            final String part = iter.next();
            target = context.member(target, part);
        }
        return target;
    }

    @Override
    public Set<Name> names() {

        return ImmutableSet.of(name);
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

        return closure.has(name.first());
    }

    @Override
    public <T> T visit(final ExpressionVisitor<T> visitor) {

        return visitor.visitNameConstant(this);
    }

    @Override
    public List<Expression> expressions() {

        return Collections.emptyList();
    }

    @Override
    public Expression copy(final List<Expression> expressions) {

        assert expressions.size() == 0;
        return this;
    }

    @Override
    public String toString() {

        return name.toString();
    }

    public static Matcher<NameConstant> match() {

        return match(p -> p);
    }

    public static <R> Matcher<R> match(final Function<NameConstant, R> then) {

        return e -> {
            if(e instanceof NameConstant) {
                return then.apply((NameConstant) e);
            } else {
                return null;
            }
        };
    }
}
