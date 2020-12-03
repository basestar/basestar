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
import io.basestar.expression.arithmetic.Add;
import io.basestar.expression.exception.InvalidAggregateException;
import io.basestar.util.Name;
import lombok.Data;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

@Data
public class Sum implements Aggregate {

    public static final String NAME = "sum";

    private final Expression input;

    public static Aggregate create(final List<Expression> args) {

        if(args.size() == 1) {
            return new Sum(args.get(0));
        } else {
            throw new InvalidAggregateException(NAME);
        }
    }

    @Override
    public Object evaluate(final Context context, final Stream<? extends Map<String, Object>> values) {

        return values.map(v -> input.evaluate(context.with(v))).reduce(Add::apply).orElse(null);
    }

    @Override
    public Sum bind(final Context context, final Renaming root) {

        final Expression boundInput = this.input.bind(context, root);
        if(boundInput == this.input) {
            return this;
        } else {
            return new Sum(boundInput);
        }
    }

    @Override
    public Set<Name> names() {

        return input.names();
    }

    @Override
    public <T> T visit(final ExpressionVisitor<T> visitor) {

        return visitor.visitSum(this);
    }

    @Override
    public List<Expression> expressions() {

        return ImmutableList.of(input);
    }

    @Override
    public Expression copy(final List<Expression> expressions) {

        return new Sum(expressions.get(0));
    }

    @Override
    public String toString() {

        return NAME + "(" + input + ")";
    }
}
