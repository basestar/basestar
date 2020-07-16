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

import io.basestar.schema.InstanceSchema;
import io.basestar.schema.Link;
import io.basestar.schema.Property;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseCollection;
import io.basestar.schema.use.UseInstance;
import io.basestar.schema.use.UseMap;
import io.basestar.util.Name;
import lombok.Builder;
import lombok.NonNull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

@Builder(builderClassName = "Builder")
public class ExpandTransform implements Transform<Dataset<Row>, Dataset<Row>> {

    @NonNull
    private final Function<Name, Dataset<Row>> sources;

    @NonNull
    private final InstanceSchema schema;

    @NonNull
    private final Set<Name> expand;

    @Override
    public Dataset<Row> accept(final Dataset<Row> input) {

        return instanceExpand(input, schema, expand);
    }

    public Dataset<Row> instanceExpand(final Dataset<Row> input, final InstanceSchema schema, final Set<Name> expand) {

        Dataset<Row> output = input;
        final Map<String, Set<Name>> branches = Name.branch(expand);
        for(final Map.Entry<String, Property> entry : schema.getProperties().entrySet()) {
            final Set<Name> branch = branches.get(entry.getKey());
            if(branch != null) {
                output = referenceExpand(output, entry.getValue().getType(), branch);
            }
        }
        if(schema instanceof Link.Resolver) {
            for(final Map.Entry<String, Link> entry : ((Link.Resolver)schema).getLinks().entrySet()) {
                final Set<Name> branch = branches.get(entry.getKey());
                if(branch != null) {
                    output = linkExpand(output, entry.getValue(), branch);
                }
            }
        }
        // FIXME: decide if transients get turned into properties with transient = true
//        if(schema instanceof Transient.Resolver) {
//            for (final Map.Entry<String, Transient> entry : ((Transient.Resolver) schema).getTransients().entrySet()) {
//                final Set<Name> branch = branches.get(entry.getKey());
//                if(branch != null) {
//
//                }
//            }
//        }
        return null;
    }

    private Dataset<Row> linkExpand(final Dataset<Row> output, final Link value, final Set<Name> branch) {

//        final ObjectSchema targetSchema = value.getSchema();
//        final Dataset<Row> linkTarget = sources.apply(name);
//        final Expression expression = value.getExpression();
//        expression.visit(new SparkExpressionVisitor(name -> {
//            if(name.size() > 2 && Reserved.THIS.equals(name.first())) {
//
//            }
//        }));
//
//        output.withColumn(joinUsing, )
        return null;
    }

    private Dataset<Row> referenceExpand(final Dataset<Row> input, final Use<?> type, final Set<Name> expand) {

        return type.visit(new Use.Visitor.Defaulting<Dataset<Row>>() {

            @Override
            public Dataset<Row> visitDefault(final Use<?> type) {

                return input;
            }

            @Override
            public <T> Dataset<Row> visitCollection(final UseCollection<T, ? extends Collection<T>> type) {

                // FIXME
                return visitDefault(type);
            }

            @Override
            public <T> Dataset<Row> visitMap(final UseMap<T> type) {

                // FIXME
                return visitDefault(type);
            }

            @Override
            public Dataset<Row> visitInstance(final UseInstance type) {

                return instanceExpand(input, type.getSchema(), expand);
            }
        });
    }
}
