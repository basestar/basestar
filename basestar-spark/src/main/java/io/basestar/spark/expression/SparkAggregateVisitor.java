package io.basestar.spark.expression;

/*-
 * #%L
 * basestar-spark
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
import io.basestar.util.Path;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

import java.util.function.Function;

@RequiredArgsConstructor
public class SparkAggregateVisitor implements AggregateVisitor<Column> {

    private final Function<Expression, Column> columnResolver;

    @Override
    public Column visitSum(final Sum aggregate) {

        final Column input = columnResolver.apply(aggregate.getInput());
        return functions.sum(input);
    }

    @Override
    public Column visitMin(final Min aggregate) {

        final Column input = columnResolver.apply(aggregate.getInput());
        return functions.min(input);
    }

    @Override
    public Column visitMax(final Max aggregate) {

        final Column input = columnResolver.apply(aggregate.getInput());
        return functions.max(input);
    }

    @Override
    public Column visitAvg(final Avg aggregate) {

        final Column input = columnResolver.apply(aggregate.getInput());
        return functions.avg(input);
    }

    @Override
    public Column visitCount(final Count count) {

        final Column input = columnResolver.apply(new Constant(true));
        return functions.count(input);
    }
}
