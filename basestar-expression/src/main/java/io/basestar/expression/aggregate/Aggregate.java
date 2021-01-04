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

import com.google.common.collect.ImmutableMap;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.call.LambdaCall;
import io.basestar.util.Immutable;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public interface Aggregate extends Expression {

    Map<String, Factory> REGISTRY = ImmutableMap.<String, Factory>builder()
            .put(Avg.NAME.toLowerCase(), Avg::create)
            .put(CollectArray.NAME.toLowerCase(), CollectArray::create)
            .put(Count.NAME.toLowerCase(), Count::create)
            .put(Max.NAME.toLowerCase(), Max::create)
            .put(Min.NAME.toLowerCase(), Min::create)
            .put(Sum.NAME.toLowerCase(), Sum::create)
            .build();

    Object evaluate(Stream<Context> context);

    interface Factory {

        Aggregate create(List<Expression> args);
    }

    static Factory factory(final String name) {

        return REGISTRY.get(name.toLowerCase());
    }

    @Override
    default boolean isAggregate() {

        return true;
    }

    Object append(Object value, Object add);

    Object remove(Object value, Object sub);

    boolean isAppendable();

    boolean isRemovable();

    default List<Aggregate> components() {

        return Immutable.list(this);
    }

    default Object fromComponents(final Object ... values) {

        assert values.length == 1;
        return values[0];
    }

    @Override
    default Object evaluate(final Context context) {

        throw new UnsupportedOperationException("Aggregate cannot be evaluated in this context");
    }

    @Override
    default boolean isConstant(final Closure closure) {

        return false;
    }

    @Override
    default int precedence() {

        return LambdaCall.PRECEDENCE;
    }

    @Override
    default String token() {

        return LambdaCall.TOKEN;
    }

    @Override
    Aggregate copy(List<Expression> expressions);
}
