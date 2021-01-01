package io.basestar.expression.aggregate;

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
import com.google.common.collect.Ordering;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.Renaming;
import io.basestar.expression.exception.InvalidAggregateException;
import io.basestar.expression.type.Values;
import io.basestar.util.Name;
import lombok.Data;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

@Data
public class Min implements Aggregate {

    public static final String NAME = "min";

    private final Expression input;

    public static Aggregate create(final List<Expression> args) {

        if(args.size() == 1) {
            return new Min(args.get(0));
        } else {
            throw new InvalidAggregateException(NAME);
        }
    }

    @Override
    public Object evaluate(final Stream<Context> contexts) {

        return Ordering.from(Values::compare).min(contexts.map(input::evaluate).iterator());
    }

    @Override
    public Object append(final Object value, final Object add) {

        if(Values.compare(value, add) < 0) {
            return add;
        } else {
            return value;
        }
    }

    @Override
    public Object remove(final Object value, final Object sub) {

        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isAppendable() {

        return true;
    }

    @Override
    public boolean isRemovable() {

        return false;
    }

    @Override
    public Min bind(final Context context, final Renaming root) {

        final Expression boundInput = this.input.bind(context, root);
        if(boundInput == this.input) {
            return this;
        } else {
            return new Min(boundInput);
        }
    }

    @Override
    public Set<Name> names() {

        return input.names();
    }

    @Override
    public <T> T visit(final ExpressionVisitor<T> visitor) {

        return visitor.visitMin(this);
    }

    @Override
    public List<Expression> expressions() {

        return ImmutableList.of(input);
    }

    @Override
    public Aggregate copy(final List<Expression> expressions) {

        return new Min(expressions.get(0));
    }

    @Override
    public String toString() {

        return NAME + "(" + input + ")";
    }
}
