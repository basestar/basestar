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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Expression;
import io.basestar.schema.*;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.use.*;
import io.basestar.spark.expression.SparkExpressionVisitor;
import io.basestar.spark.resolver.ColumnResolver;
import io.basestar.spark.resolver.SchemaResolver;
import io.basestar.spark.util.ScalaUtils;
import io.basestar.spark.util.SparkSchemaUtils;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.util.*;

@Builder(builderClassName = "Builder")
public class ExpandTransform implements Transform<Dataset<Row>, Dataset<Row>> {

    private static final String REF_ID_COLUMN = Reserved.PREFIX + Reserved.PREFIX + ObjectSchema.ID;

    @NonNull
    private final SchemaResolver resolver;

    @NonNull
    private final ColumnResolver<Row> columnResolver;

    @NonNull
    private final LinkableSchema schema;

    @NonNull
    private final Set<Name> expand;

    @lombok.Builder(builderClassName = "Builder")
    ExpandTransform(@NonNull final SchemaResolver resolver, @Nullable final ColumnResolver<Row> columnResolver,
                    @NonNull final LinkableSchema schema, @NonNull final Set<Name> expand) {

        this.resolver = Nullsafe.require(resolver);
        this.columnResolver = Nullsafe.orDefault(columnResolver, ColumnResolver::nested);
        this.schema = Nullsafe.require(schema);
        this.expand = Immutable.copy(expand);
    }

    @Override
    public Dataset<Row> accept(final Dataset<Row> input) {

        return instanceExpand(input, schema, expand);
    }

    private Dataset<Row> instanceExpand(final Dataset<Row> input, final LinkableSchema schema, final Set<Name> expand) {

        Dataset<Row> output = resolver.conform(schema, expand, input);

        output = propertiesExpand(output, schema, expand);

        final Map<String, Set<Name>> branches = Name.branch(expand);
        for (final Map.Entry<String, Link> entry : schema.getLinks().entrySet()) {
            final Set<Name> branch = branches.get(entry.getKey());
            if (branch != null) {
                output = linkExpand(schema, output, entry.getValue(), branch);
            }
        }

        return output;
    }

    // NOTE: try to do some of the grouping steps using collect_list etc and compare performance

    private Dataset<Row> linkExpand(final LinkableSchema schema, final Dataset<Row> input, final Link link, final Set<Name> expand) {

        final String rootIdColumn = this.schema.id();
        final Use<?> rootIdType = this.schema.typeOfId();

        return linkExpand(schema, input, link, expand, rootIdColumn, rootIdType);
    }

    @SuppressWarnings("unchecked")
    private <T> Dataset<Row> linkExpand(final LinkableSchema schema, final Dataset<Row> input, final Link link, final Set<Name> expand, final String rootIdColumn, final Use<T> rootIdType) {

        final String linkName = link.getName();

        final LinkableSchema linkSchema = link.getSchema();

        final Dataset<Row> linkInput = resolver.resolveAndConform(linkSchema, expand);

        final InferenceContext inferenceContext = new InferenceContext.Overlay(
                new InferenceContext.FromSchema(linkSchema),
                ImmutableMap.of(Reserved.THIS, new InferenceContext.FromSchema(schema))
        );

        final Expression expression = link.getExpression();
        final Column joinCondition = expression.visit(new SparkExpressionVisitor(name -> {
            if (Reserved.THIS.equals(name.first())) {
                final Name next = name.withoutFirst();
                return columnResolver.resolve(input, next);
            } else {
                return columnResolver.resolve(linkInput, name);
            }
        }, inferenceContext));

        final Dataset<Tuple2<Row, Row>> joined = input.joinWith(linkInput, joinCondition, "left_outer");

        final SortTransform<Tuple2<Row, Row>> sortTransform = SortTransform.<Tuple2<Row, Row>>builder()
                .sort(link.getEffectiveSort())
                .columnResolver((d, name) -> ColumnResolver.nested(d.col("_2"), name))
                .build();

        final Dataset<Tuple2<Row, Row>> sorted = sortTransform.accept(joined);

        final MapFunction<Tuple2<Row, Row>, T> groupBy = v -> (T) SparkSchemaUtils.get(v._1(), rootIdColumn);
        final KeyValueGroupedDataset<T, Tuple2<Row, Row>> grouped = sorted.groupByKey(groupBy, (Encoder<T>)SparkSchemaUtils.encoder(rootIdType));

        final MapGroupsFunction<T, Tuple2<Row, Row>, Row> collect = (id, iter) -> {

            Row root = null;
            final List<Row> values = new ArrayList<>();
            while (iter.hasNext()) {
                final Tuple2<Row, Row> tuple = iter.next();
                final Row _1 = tuple._1();
                final Row _2 = tuple._2();
                if (_2 != null) {
                    values.add(_2);
                }
//                assert (root == null || root.equals(_1));
                root = _1;
            }
            assert root != null;
            final Object outputValue;
            if (link.isSingle()) {
                outputValue = values.isEmpty() ? null : values.get(0);
            } else {
                outputValue = ScalaUtils.asScalaSeq(values);
            }
            return SparkSchemaUtils.transform(root, (field, value) -> field.name().equals(linkName) ? outputValue : value);
        };

        return grouped.mapGroups(collect, RowEncoder.apply(input.schema()));
    }

    private Dataset<Row> propertiesExpand(final Dataset<Row> input, final Property.Resolver schema, final Set<Name> expand) {

        final String rootIdColumn = this.schema.id();
        final Use<?> rootIdType = this.schema.typeOfId();

        return propertiesExpand(input, schema, expand, rootIdColumn, rootIdType);
    }

    private <T> Dataset<Row> propertiesExpand(final Dataset<Row> input, final Property.Resolver schema, final Set<Name> expand, final String rootIdColumn, final Use<T> rootIdType) {

        // Safe, only string and binary types are expected
        @SuppressWarnings("unchecked")
        final Encoder<T> encoder = (Encoder<T>)SparkSchemaUtils.encoder(rootIdType);

        final Map<String, Set<Name>> branches = Name.branch(expand);
        final Set<RequiredRef> requiredRefs = new HashSet<>();
        schema.getProperties().forEach((name, property) -> {
            final Set<Name> branch = branches.get(name);
            if(branch != null) {
                requiredRefs.addAll(requiredRefs(property.getType(), branch));
            }
        });

        Dataset<Row> output = input;
        for (final RequiredRef requiredRef : requiredRefs) {

            final Dataset<Row> refInput = cache(resolver.resolveAndConform(requiredRef.getSchema(), requiredRef.getExpand()));

            final StructField refIdField = SparkSchemaUtils.field(REF_ID_COLUMN, DataTypes.StringType);

            final FlatMapFunction<Row, Row> refIds = row -> {

                final Set<String> ids = new HashSet<>();
                schema.getProperties().forEach((name, property) -> {
                    final Set<Name> branch = branches.get(name);
                    if(branch != null) {
                        ids.addAll(refIds(property.getType(), branch, SparkSchemaUtils.get(row, name)));
                    }
                });
                // Make sure we always have at least one output row else this will act like an inner join
                if(ids.isEmpty()) {
                    ids.add(null);
                }

                return ids.stream().map(id -> SparkSchemaUtils.append(row, refIdField, id)).iterator();
            };

            final StructType flatMapType = SparkSchemaUtils.append(output.schema(), refIdField);
            final Dataset<Row> withRefIds = output.flatMap(refIds, RowEncoder.apply(flatMapType));

            final String refIdColumn = requiredRef.getSchema().id();
            final Column joinCondition = refInput.col(refIdColumn).equalTo(withRefIds.col(REF_ID_COLUMN));
            final Dataset<Tuple2<Row, Row>> joined = withRefIds.joinWith(refInput, joinCondition, "left_outer");

            final MapFunction<Tuple2<Row, Row>, T> groupBy = v -> rootIdType.create(SparkSchemaUtils.get(v._1(), rootIdColumn));
            final KeyValueGroupedDataset<T, Tuple2<Row, Row>> grouped = joined.groupByKey(groupBy, encoder);

            final MapGroupsFunction<T, Tuple2<Row, Row>, Row> collect = (id, iter) -> {

                Row root = null;
                final Map<String, Row> lookup = new HashMap<>();
                while (iter.hasNext()) {
                    final Tuple2<Row, Row> tuple = iter.next();
                    final Row _1 = SparkSchemaUtils.remove(tuple._1(), REF_ID_COLUMN);
                    final Row _2 = tuple._2();
                    if (_2 != null) {
                        lookup.put((String) SparkSchemaUtils.get(_2, refIdColumn), _2);
                    }
//                    assert (root == null || root.equals(_1));
                    root = _1;
                }
                assert root != null;

                return SparkSchemaUtils.transform(root, (field, oldValue) -> {
                    final String name = field.name();
                    final Set<Name> branch = branches.get(name);
                    if(branch != null) {
                        final Property property = schema.getProperty(name, true);
                        if(property != null) {
                            return applyRefs(property.getType(), branch, oldValue, lookup);
                        }
                    }
                    return oldValue;
                });
            };

            output = grouped.mapGroups(collect, RowEncoder.apply(output.schema()));
        }

        return output;
    }

    protected Dataset<Row> cache(final Dataset<Row> ds) {

        return ds.cache();
    }

    @Data
    private static class RequiredRef {

        private final LinkableSchema schema;

        private final Set<Name> expand;
    }

    private static Set<RequiredRef> requiredRefs(final Use<?> type, final Set<Name> expand) {

        return type.visit(new Use.Visitor.Defaulting<Set<RequiredRef>>() {

            @Override
            public <T> Set<RequiredRef> visitDefault(final Use<T> type) {

                return ImmutableSet.of();
            }

            @Override
            public <V, T extends Collection<V>> Set<RequiredRef> visitCollection(final UseCollection<V, T> type) {

                return requiredRefs(type.getType(), expand);
            }

            @Override
            public <T> Set<RequiredRef> visitMap(final UseMap<T> type) {

                final Map<String, Set<Name>> branches = Name.branch(expand);
                final Set<Name> branch = branches.get(UseMap.EXPAND_WILDCARD);
                if (branch != null) {
                    return requiredRefs(type.getType(), branch);
                } else {
                    return ImmutableSet.of();
                }
            }

            @Override
            public Set<RequiredRef> visitStruct(final UseStruct type) {

                final Set<RequiredRef> results = new HashSet<>();
                final Map<String, Set<Name>> branches = Name.branch(expand);
                for (final Map.Entry<String, Property> entry : type.getSchema().getProperties().entrySet()) {
                    final String name = entry.getKey();
                    final Set<Name> branch = branches.get(name);
                    if (branch != null) {
                        results.addAll(requiredRefs(entry.getValue().getType(), branch));
                    }
                }
                return results;
            }

            @Override
            public Set<RequiredRef> visitRef(final UseRef type) {

                return ImmutableSet.of(new RequiredRef(type.getSchema(), expand));
            }
        });
    }

    private static Set<String> refIds(final Use<?> type, final Set<Name> expand, final Object input) {

        if (input == null) {
            return Collections.emptySet();
        } else {

            return type.visit(new Use.Visitor.Defaulting<Set<String>>() {

                @Override
                public <T> Set<String> visitDefault(final Use<T> type) {

                    return Collections.emptySet();
                }

                @Override
                public <V, T extends Collection<V>> Set<String> visitCollection(final UseCollection<V, T> type) {

                    if (input instanceof scala.collection.Iterable<?>) {
                        final Set<String> results = new HashSet<>();
                        ((scala.collection.Iterable<?>) input)
                                .foreach(ScalaUtils.scalaFunction(v -> results.addAll(refIds(type.getType(), expand, v))));
                        return results;
                    } else {
                        throw new IllegalStateException();
                    }
                }

                @Override
                public <T> Set<String> visitMap(final UseMap<T> type) {

                    if (input instanceof scala.collection.Map<?, ?>) {
                        final Map<String, Set<Name>> branches = Name.branch(expand);
                        final Set<Name> branch = branches.get(UseMap.EXPAND_WILDCARD);
                        final Set<String> results = new HashSet<>();
                        if (branch != null) {
                            ((scala.collection.Map<?, ?>) input)
                                    .foreach(ScalaUtils.scalaFunction(t -> results.addAll(refIds(type.getType(), branch, t._2()))));
                        }
                        return results;
                    } else {
                        throw new IllegalStateException();
                    }
                }

                @Override
                public Set<String> visitStruct(final UseStruct type) {

                    if (input instanceof Row) {
                        final Row row = (Row) input;
                        final Set<String> results = new HashSet<>();
                        final Map<String, Set<Name>> branches = Name.branch(expand);
                        for (final Map.Entry<String, Property> entry : type.getSchema().getProperties().entrySet()) {
                            final String name = entry.getKey();
                            final Set<Name> branch = branches.get(name);
                            if (branch != null) {
                                results.addAll(refIds(entry.getValue().getType(), branch, SparkSchemaUtils.get(row, name)));
                            }
                        }
                        return results;
                    } else {
                        throw new IllegalStateException();
                    }
                }

                @Override
                public Set<String> visitRef(final UseRef type) {

                    if (input instanceof Row) {
                        final Row row = (Row) input;
                        final String id = (String) SparkSchemaUtils.get(row, ObjectSchema.ID);
                        return id == null ? ImmutableSet.of() : ImmutableSet.of(id);
                    } else {
                        throw new IllegalStateException();
                    }
                }
            });
        }
    }

    private static Object applyRefs(final Use<?> type, final Set<Name> expand, final Object input, final Map<String, Row> lookup) {

        if (input == null) {
            return null;
        } else {
            return type.visit(new Use.Visitor.Defaulting<Object>() {

                @Override
                public <T> Object visitDefault(final Use<T> type) {

                    return input;
                }

                @Override
                public <V, T extends Collection<V>> scala.collection.Iterable<?> visitCollection(final UseCollection<V, T> type) {

                    if (input instanceof scala.collection.Iterable<?>) {
                        final List<Object> results = new ArrayList<>();
                        ((scala.collection.Iterable<?>) input)
                                .foreach(ScalaUtils.scalaFunction(v -> results.add(applyRefs(type.getType(), expand, v, lookup))));
                        return ScalaUtils.asScalaSeq(results);
                    } else {
                        throw new IllegalStateException();
                    }
                }

                @Override
                public <T> scala.collection.Map<?, ?> visitMap(final UseMap<T> type) {

                    if (input instanceof scala.collection.Map<?, ?>) {
                        final Map<String, Object> results = new HashMap<>();
                        final Map<String, Set<Name>> branches = Name.branch(expand);
                        final Set<Name> branch = branches.get(UseMap.EXPAND_WILDCARD);
                        if (branch != null) {
                            ((scala.collection.Map<?, ?>) input)
                                    .foreach(ScalaUtils.scalaFunction(t -> results.put((String)t._1(), applyRefs(type.getType(), branch, t._2(), lookup))));
                        }
                        return ScalaUtils.asScalaMap(results);
                    } else {
                        throw new IllegalStateException();
                    }
                }

                @Override
                public Row visitStruct(final UseStruct type) {

                    if (input instanceof Row) {
                        final Row row = (Row) input;
                        final Map<String, Set<Name>> branches = Name.branch(expand);
                        return SparkSchemaUtils.transform(row, (field, oldValue) -> {
                            final String name = field.name();
                            final Set<Name> branch = branches.get(name);
                            if(branch != null) {
                                final Property property = type.getSchema().requireProperty(name, true);
                                return applyRefs(property.getType(), branch, oldValue, lookup);
                            } else {
                                return oldValue;
                            }
                        });
                    } else {
                        throw new IllegalStateException();
                    }
                }

                @Override
                public Row visitRef(final UseRef type) {

                    if (input instanceof Row) {
                        final Row row = (Row) input;
                        final String id = (String) SparkSchemaUtils.get(row, ObjectSchema.ID);
                        if (id != null && lookup.get(id) != null) {
                            return lookup.get(id);
                        } else {
                            return (Row)input;
                        }
                    } else {
                        throw new IllegalStateException();
                    }
                }
            });
        }
    }
}
