package io.basestar.spark.resolver;

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
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.ViewSchema;
import io.basestar.schema.use.Use;
import io.basestar.spark.database.QueryChain;
import io.basestar.spark.transform.*;
import io.basestar.spark.util.SparkSchemaUtils;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public interface SchemaResolver {

    Dataset<Row> resolve(InstanceSchema schema, ColumnResolver<Row> columnResolver, Set<Name> expand);

    default Dataset<Row> resolve(final InstanceSchema schema, final ColumnResolver<Row> columnResolver) {

        return resolve(schema, columnResolver, ImmutableSet.of());
    }

    default Dataset<Row> resolveAndConform(final InstanceSchema schema, final ColumnResolver<Row> columnResolver, final Set<Name> expand) {

        return conform(schema, expand, resolve(schema, columnResolver, expand));
    }

    default Dataset<Row> conform(final InstanceSchema schema, final Set<Name> expand, final Dataset<Row> input) {

        final SchemaTransform schemaTransform = SchemaTransform.builder().schema(schema).expand(expand).build();
        return schemaTransform.accept(input);
    }

    static SchemaResolver.Automatic automatic(final Automatic.Resolver resolver) {

        return new Automatic(resolver);
    }

    default SchemaResolver then(final Transform<Dataset<Row>, Dataset<Row>> transform) {

        return (schema, columnResolver, expand) -> transform.accept(SchemaResolver.this.resolve(schema, columnResolver, expand));
    }

    @RequiredArgsConstructor
    class Automatic implements SchemaResolver {

        public interface Resolver extends Serializable {

            Dataset<Row> resolve(InstanceSchema schema);
        }

        private final Resolver resolver;

        @Override
        public Dataset<Row> resolve(final InstanceSchema schema, final ColumnResolver<Row> columnResolver, final Set<Name> expand) {

            if(schema instanceof ObjectSchema) {

                final ObjectSchema objectSchema = (ObjectSchema)schema;
                final Set<Name> mergedExpand = mergedExpand(expand, objectSchema.getExpand());
                final ExpressionTransform expressionTransform = ExpressionTransform.builder().schema(objectSchema).expand(mergedExpand).build();
                final SchemaTransform schemaTransform = SchemaTransform.builder().schema(schema).build();
                final ExpandTransform expandTransform = ExpandTransform.builder().resolver(this).columnResolver(columnResolver).schema(schema).expand(mergedExpand).build();
                final Dataset<Row> base = Nullsafe.require(resolver.resolve(schema));
                return schemaTransform.then(expressionTransform).then(expandTransform).accept(base);

            } else if(schema instanceof ViewSchema) {

                final ViewSchema viewSchema = (ViewSchema)schema;
                final Set<Name> mergedExpand = mergedExpand(expand, viewSchema.getExpand());
                final ViewTransform viewTransform = ViewTransform.builder().schema(viewSchema).build();
                final ExpandTransform expandTransform = ExpandTransform.builder().resolver(this).columnResolver(columnResolver).schema(schema).expand(mergedExpand).build();
                final ViewSchema.From from = viewSchema.getFrom();
                final Dataset<Row> base = resolve(from.getSchema(), columnResolver, from.getExpand());
                return viewTransform.then(expandTransform).accept(base);

            } else {

                throw new IllegalStateException("Cannot resolve dataset for schema: " + schema.getQualifiedName());
            }
        }

        private Set<Name> mergedExpand(final Set<Name> expand, final Set<Name> defaultExpand) {

            if(expand.contains(QueryChain.DEFAULT_EXPAND)) {
                final Set<Name> mergedExpand = new HashSet<>(expand);
                mergedExpand.remove(QueryChain.DEFAULT_EXPAND);
                mergedExpand.addAll(defaultExpand);
                return mergedExpand;
            } else {
                return expand;
            }
        }
    }

    @RequiredArgsConstructor
    class Overlaying implements SchemaResolver {

        public interface Combiner extends Serializable {

            default Column condition(final InstanceSchema schema, final Dataset<Row> baseline, final Dataset<Row> overlay) {

                return baseline.col(schema.id()).equalTo(overlay.col(schema.id()));
            }

            default String joinType() {

                return "full_outer";
            }

            default Map<String, Use<?>> extraMetadataSchema(final InstanceSchema schema) {

                return Collections.emptyMap();
            }

            default StructType outputType(final InstanceSchema schema) {

                return SparkSchemaUtils.structType(schema, schema.getExpand(), extraMetadataSchema(schema));
            }

            default Row combine(final InstanceSchema schema, final StructType structType, final Row baseline, final Row overlay) {

                final Map<String, Object> baselineInstance = SparkSchemaUtils.fromSpark(schema, schema.getExpand(), baseline);
                final Map<String, Object> overlayInstance = SparkSchemaUtils.fromSpark(schema, schema.getExpand(), overlay);
                final Map<String, Object> resultInstance = combine(schema, baselineInstance, overlayInstance);
                return SparkSchemaUtils.toSpark(schema, schema.getExpand(), structType, resultInstance);
            }

             Map<String, Object> combine(InstanceSchema schema, Map<String, Object> baseline, Map<String, Object> overlay);
        }

        private final SchemaResolver baseline;

        private final SchemaResolver overlay;

        private final Combiner combiner;

        @Override
        public Dataset<Row> resolve(final InstanceSchema schema, final ColumnResolver<Row> columnResolver, final Set<Name> expand) {

            final Dataset<Row> baseline = this.baseline.resolve(schema, columnResolver, expand);
            final Dataset<Row> overlay = this.overlay.resolve(schema, columnResolver, expand);
            final Combiner combiner = this.combiner;
            final StructType structType = combiner.outputType(schema);
            final Column condition = combiner.condition(schema, baseline, overlay);
            return baseline.joinWith(overlay, condition, combiner.joinType())
                    .flatMap((FlatMapFunction<Tuple2<Row, Row>, Row>) v -> {

                        final Row result = combiner.combine(schema, structType, v._1(), v._2());
                        if(result == null) {
                            return Collections.emptyIterator();
                        } else {
                            return Collections.singleton(result).iterator();
                        }

                    }, RowEncoder.apply(structType));
        }
    }
}
