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

import io.basestar.expression.Expression;
import io.basestar.schema.*;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseString;
import io.basestar.spark.expression.SparkExpressionVisitor;
import io.basestar.spark.util.SparkSchemaUtils;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Set;

public class ExpressionTransform implements Transform<Dataset<Row>, Dataset<Row>> {

    private final InstanceSchema schema;

    private final Set<Name> expand;

    @lombok.Builder(builderClassName = "Builder")
    ExpressionTransform(final InstanceSchema schema, final Set<Name> expand) {

        this.schema = Nullsafe.require(schema);
        this.expand = Nullsafe.orDefault(expand);
    }

    @Override
    public Dataset<Row> accept(final Dataset<Row> input) {

        Dataset<Row> output = input;
        if(schema instanceof ObjectSchema) {
            final ObjectSchema objectSchema = (ObjectSchema)schema;
            final Id id = objectSchema.getId();
            if (id != null && id.getExpression() != null) {
                final Column col = apply(input, id.getExpression(), UseString.DEFAULT);
                output = output.withColumn(ObjectSchema.ID, col);
            }
        }
        for(final Property property : schema.getProperties().values()) {
            if(property.getExpression() != null) {
                output = output.withColumn(property.getName(), apply(output, property.getExpression(), property.getType()));
            }
        }
        return output;
    }

    private Column apply(final Dataset<Row> ds, final Expression expression, final Use<?> type) {

        return SparkSchemaUtils.cast(visitor(ds).visit(expression), type, expand);
    }

    private SparkExpressionVisitor visitor(final Dataset<Row> ds) {

        return new SparkExpressionVisitor(path -> {
            assert path.size() == 2 && path.isChild(Name.of(Reserved.THIS));
            return ds.col(path.get(1));
        });
    }
}
