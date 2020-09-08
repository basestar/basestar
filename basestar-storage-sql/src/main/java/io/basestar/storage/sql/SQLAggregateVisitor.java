package io.basestar.storage.sql;

/*-
 * #%L
 * basestar-storage-sql
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

import io.basestar.expression.Expression;
import io.basestar.expression.aggregate.*;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.type.Values;
import lombok.RequiredArgsConstructor;
import org.jooq.Field;
import org.jooq.impl.DSL;

import java.util.function.Function;

@RequiredArgsConstructor
public class SQLAggregateVisitor implements AggregateVisitor.Defaulting<Field<?>> {

    private final Function<Expression, Field<?>> columnResolver;

    @Override
    public Field<?> visitDefault(final Aggregate aggregate) {

        throw new UnsupportedOperationException("Aggregate " + aggregate.getClass() + " not supported");
    }

    @Override
    public Field<?> visitSum(final Sum aggregate) {

        final Field<? extends Number> input = SQLUtils.cast(columnResolver.apply(aggregate.getInput()), Number.class);
        return DSL.sum(input);
    }

    @Override
    public Field<?> visitMin(final Min aggregate) {

        final Field<?> input = columnResolver.apply(aggregate.getInput());
        return DSL.min(input);
    }

    @Override
    public Field<?> visitMax(final Max aggregate) {

        final Field<?> input = columnResolver.apply(aggregate.getInput());
        return DSL.max(input);
    }

    @Override
    public Field<?> visitAvg(final Avg aggregate) {

        final Field<? extends Number> input = SQLUtils.cast(columnResolver.apply(aggregate.getInput()), Number.class);
        return DSL.avg(input);
    }

    @Override
    public Field<?> visitCount(final Count aggregate) {

        if(aggregate.getPredicate() instanceof Constant) {
            final boolean value = Values.isTruthy(((Constant) aggregate.getPredicate()).getValue());
            if(value) {
                return DSL.count();
            } else {
                return DSL.inline(0);
            }
        } else {
            final Field<?> input = columnResolver.apply(aggregate.getPredicate());
            return DSL.count(DSL.nullif(input, false));
        }
    }

    @Override
    public Field<?> visitCollectArray(final CollectArray aggregate) {

        throw new UnsupportedOperationException();
    }

    public Field<?> field(final Aggregate aggregate) {

        return SQLUtils.field(visit(aggregate));
    }
}
