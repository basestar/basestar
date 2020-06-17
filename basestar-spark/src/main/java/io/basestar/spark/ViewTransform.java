package io.basestar.spark;

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

import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.aggregate.Aggregate;
import io.basestar.expression.aggregate.AggregateExtractingVisitor;
import io.basestar.schema.*;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseBoolean;
import io.basestar.spark.expression.SparkAggregateVisitor;
import io.basestar.spark.expression.SparkExpressionVisitor;
import io.basestar.util.Nullsafe;
import io.basestar.util.Path;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;


public class ViewTransform implements Transform<Dataset<Row>, Dataset<Row>> {

    final ViewSchema schema;

    @lombok.Builder(builderClassName = "Builder")
    ViewTransform(final ViewSchema schema) {

        this.schema = schema;
    }

    @Override
    public Dataset<Row> accept(final Dataset<Row> input) {

        Dataset<Row> output = input;
        if(schema.getWhere() != null) {
            output = output.where(apply(output, schema.getWhere(), UseBoolean.DEFAULT));
        }

        final Context context = Context.init();

        final AggregateExtractingVisitor visitor = new AggregateExtractingVisitor();
        final Map<String, Expression> columns = new HashMap<>();
        schema.getSelect().forEach((name, prop) -> {
            final Expression expr = Nullsafe.require(prop.getExpression()).bind(context);
            columns.put(name, visitor.visit(expr));
        });
        final Map<String, Aggregate> aggregates = visitor.getAggregates();

        final List<Column> selectColumns = new ArrayList<>();

        if(!(aggregates.isEmpty() && schema.getGroup().isEmpty())) {

            final List<Column> groupColumns = new ArrayList<>();
            for(final Map.Entry<String, Property> entry : schema.getGroup().entrySet()) {
                final String name = entry.getKey();
                final Property prop = entry.getValue();
                final Expression expr = Nullsafe.require(prop.getExpression()).bind(context);
                groupColumns.add(apply(output, expr, prop.getType()).as(name));
            }
            final RelationalGroupedDataset groupedOutput = output.groupBy(groupColumns.toArray(new Column[0]));

            final List<Column> aggColumns = new ArrayList<>();
            for(final Map.Entry<String, Aggregate> entry : aggregates.entrySet()) {
                final String name = entry.getKey();
                final Aggregate agg = entry.getValue();
                aggColumns.add(apply(output, agg).as(name));
            }
            assert !aggColumns.isEmpty();
            final Column first = aggColumns.get(0);
            final Column[] rest = aggColumns.subList(1, aggColumns.size()).toArray(new Column[0]);
            output = groupedOutput.agg(first, rest);

            for(final String key : schema.getGroup().keySet()) {
                selectColumns.add(output.col(key).as(key));
            }
        }

        for(final Map.Entry<String, Expression> entry : columns.entrySet()) {
            final String name = entry.getKey();
            final Expression expression = entry.getValue();
            selectColumns.add(apply(output, expression).as(name));
        }

        return output.select(selectColumns.toArray(new Column[0]));
    }

    private Column apply(final Dataset<Row> ds, final Aggregate aggregate) {

        return new SparkAggregateVisitor(expression -> apply(ds, expression)).visit(aggregate);
    }

    private Column apply(final Dataset<Row> ds, final Expression expression) {

        return new SparkExpressionVisitor(columnResolver(ds)).visit(expression);
    }

    private Column apply(final Dataset<Row> ds, final Expression expression, final Use<?> type) {

        return apply(ds, expression).cast(SparkSchemaUtils.type(type, ImmutableSet.of()));
    }

    private Function<Path, Column> columnResolver(final Dataset<Row> ds) {

        return path -> {
            assert path.size() == 1;// && path.isChild(Path.of(Reserved.THIS));
            return ds.col(path.get(0));
        };
    }
}
