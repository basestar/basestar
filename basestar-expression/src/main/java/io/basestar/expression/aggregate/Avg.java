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
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.Renaming;
import io.basestar.expression.exception.InvalidAggregateException;
import io.basestar.util.Name;
import lombok.Data;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Data
public class Avg implements Aggregate {

    public static final String NAME = "avg";

    private final Expression input;

    public static Aggregate create(final List<Expression> args) {

        if(args.size() == 1) {
            return new Avg(args.get(0));
        } else {
            throw new InvalidAggregateException(NAME);
        }
    }

    @Override
    public Object evaluate(final Stream<Context> contexts) {

        return contexts.map(input::evaluate)
                .collect(Collectors.averagingDouble(value -> ((Number)value).doubleValue()));
    }

    @Override
    public Object append(final Object value, final Object add) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Object remove(final Object value, final Object sub) {

        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isAppendable() {

        return false;
    }

    @Override
    public boolean isRemovable() {

        return false;
    }

    @Override
    public List<Aggregate> components() {

        return ImmutableList.of(new Sum(input), new Count());
    }

    @Override
    public Object fromComponents(final Object... values) {

        assert values.length == 2;
        return ((Number)values[0]).doubleValue() / ((Number)values[1]).longValue();
    }

    @Override
    public Avg bind(final Context context, final Renaming root) {

        final Expression boundInput = this.input.bind(context, root);
        if(boundInput == this.input) {
            return this;
        } else {
            return new Avg(boundInput);
        }
    }

    @Override
    public Set<Name> names() {

        return input.names();
    }

    @Override
    public <T> T visit(final ExpressionVisitor<T> visitor) {

        return visitor.visitAvg(this);
    }

    @Override
    public List<Expression> expressions() {

        return ImmutableList.of(input);
    }

    @Override
    public Aggregate copy(final List<Expression> expressions) {

        return new Avg(expressions.get(0));
    }

    @Override
    public String toString() {

        return NAME + "(" + input + ")";
    }
}
