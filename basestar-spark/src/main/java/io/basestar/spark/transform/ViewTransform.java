package io.basestar.spark.transform;

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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.aggregate.Aggregate;
import io.basestar.expression.aggregate.AggregateExtractingVisitor;
import io.basestar.schema.Property;
import io.basestar.schema.ViewSchema;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseBinary;
import io.basestar.schema.use.UseBoolean;
import io.basestar.spark.expression.SparkAggregateVisitor;
import io.basestar.spark.expression.SparkExpressionVisitor;
import io.basestar.spark.util.SparkSchemaUtils;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;


public class ViewTransform implements Transform<Dataset<Row>, Dataset<Row>> {

    private final ViewSchema schema;

    private final UserDefinedFunction keyFunction;

    private final List<String> keyColumnNames;

    @lombok.Builder(builderClassName = "Builder")
    ViewTransform(final ViewSchema schema) {

        this.schema = schema;
        final List<String> keyColumnNames = new ArrayList<>(schema.getGroup());
        if(keyColumnNames.isEmpty()) {
            keyColumnNames.add(schema.getFrom().getSchema().id());
        }
        this.keyFunction = functions.udf(
                (UDF1<Row, byte[]>) row -> {
                    final List<Object> values = new ArrayList<>();
                    keyColumnNames.forEach(key -> values.add(SparkSchemaUtils.get(row, key)));
                    return UseBinary.binaryKey(values);
                },
                DataTypes.BinaryType
        );
        this.keyColumnNames = ImmutableList.copyOf(keyColumnNames);
    }

    @Override
    public Dataset<Row> accept(final Dataset<Row> input) {

        final Context context = Context.init();

        Dataset<Row> output = input;
        if(schema.getWhere() != null) {
            output = output.where(apply(context, output, schema.getWhere(), UseBoolean.DEFAULT));
        }

        final AggregateExtractingVisitor visitor = new AggregateExtractingVisitor();
        final Map<String, Expression> columns = new HashMap<>();
        schema.getSelectProperties().forEach((name, prop) -> {
            final Expression expr = Nullsafe.require(prop.getExpression()).bind(context);
            columns.put(name, visitor.visit(expr));
        });
        final Map<String, Aggregate> aggregates = visitor.getAggregates();

        final List<Column> selectColumns = new ArrayList<>();

        if(!(aggregates.isEmpty() && schema.getGroup().isEmpty())) {

            final List<Column> groupColumns = new ArrayList<>();
            for(final Map.Entry<String, Property> entry : schema.getGroupProperties().entrySet()) {
                final String name = entry.getKey();
                final Property prop = entry.getValue();
                final Expression expr = Nullsafe.require(prop.getExpression());
                groupColumns.add(apply(context, output, expr, prop.getType()).as(name));
            }
            final RelationalGroupedDataset groupedOutput = output.groupBy(groupColumns.toArray(new Column[0]));

            final List<Column> aggColumns = new ArrayList<>();
            for(final Map.Entry<String, Aggregate> entry : aggregates.entrySet()) {
                final String name = entry.getKey();
                final Aggregate agg = entry.getValue();
                aggColumns.add(apply(context, output, agg).as(name));
            }
            assert !aggColumns.isEmpty();
            final Column first = aggColumns.get(0);
            final Column[] rest = aggColumns.subList(1, aggColumns.size()).toArray(new Column[0]);
            output = groupedOutput.agg(first, rest);

            for(final String key : schema.getGroup()) {
                selectColumns.add(output.col(key).as(key));
            }
        }

        final Dataset<Row> finalOutput = output;
        final Column[] keyColumns = keyColumnNames.stream().map(finalOutput::col).toArray(Column[]::new);

        // FIXME: creating struct is wasteful, UDF could be defined to take key columns as args
        output = output.withColumn(ViewSchema.KEY, keyFunction.apply(functions.struct(keyColumns)));

        for(final Map.Entry<String, Expression> entry : columns.entrySet()) {
            final String name = entry.getKey();
            final Expression expression = entry.getValue();
            selectColumns.add(apply(context, output, expression).as(name));
        }
        selectColumns.add(output.col(ViewSchema.KEY));

        return output.select(selectColumns.toArray(new Column[0]));
    }

    private Column apply(final Context context, final Dataset<Row> ds, final Aggregate aggregate) {

        return new SparkAggregateVisitor(expression -> apply(context, ds, expression)).visit(aggregate);
    }

    private Column apply(final Context context, final Dataset<Row> ds, final Expression expression) {

        return new SparkExpressionVisitor(columnResolver(ds)).visit(expression.bind(context));
    }

    private Column apply(final Context context, final Dataset<Row> ds, final Expression expression, final Use<?> type) {

        return apply(context, ds, expression).cast(SparkSchemaUtils.type(type, ImmutableSet.of()));
    }

    private Function<Name, Column> columnResolver(final Dataset<Row> ds) {

        return path -> next(ds.col(path.get(0)), path.withoutFirst());
    }

    private Column next(final Column col, final Name rest) {

        if(rest.isEmpty()) {
            return col;
        } else {
            return next(col.getField(rest.first()), rest.withoutFirst());
        }
    }
}
