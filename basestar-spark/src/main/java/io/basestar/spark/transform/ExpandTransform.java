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

import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Expression;
import io.basestar.schema.*;
import io.basestar.schema.use.*;
import io.basestar.spark.expression.SparkExpressionVisitor;
import io.basestar.spark.util.ColumnResolver;
import io.basestar.spark.util.InstanceResolver;
import io.basestar.spark.util.ScalaUtils;
import io.basestar.spark.util.SparkSchemaUtils;
import io.basestar.util.Name;
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

import java.util.*;

@Builder(builderClassName = "Builder")
public class ExpandTransform implements Transform<Dataset<Row>, Dataset<Row>> {

    private static final String REF_ID_COLUMN = Reserved.PREFIX + Reserved.ID;

    @NonNull
    private final InstanceResolver resolver;

    @NonNull
    private final InstanceSchema schema;

    @NonNull
    private final Set<Name> expand;

    @Override
    public Dataset<Row> accept(final Dataset<Row> input) {

        return instanceExpand(input, schema, expand);
    }

    private Dataset<Row> instanceExpand(final Dataset<Row> input, final InstanceSchema schema, final Set<Name> expand) {

        Dataset<Row> output = resolver.conform(schema, expand, input);

        output = propertiesExpand(output, schema, expand);

        final Map<String, Set<Name>> branches = Name.branch(expand);
        if (schema instanceof Link.Resolver) {
            for (final Map.Entry<String, Link> entry : ((Link.Resolver) schema).getLinks().entrySet()) {
                final Set<Name> branch = branches.get(entry.getKey());
                if (branch != null) {
                    output = linkExpand(output, entry.getValue(), branch);
                }
            }
        }
        return output;
    }

    // NOTE: try to do some of the grouping steps using collect_list etc and compare performance

    private Dataset<Row> linkExpand(final Dataset<Row> input, final Link link, final Set<Name> expand) {

        final String linkName = link.getName();

        final ObjectSchema linkSchema = link.getSchema();

        final Dataset<Row> linkInput = resolver.resolveAndConform(linkSchema, expand);

        final Expression expression = link.getExpression();
        final Column joinCondition = expression.visit(new SparkExpressionVisitor(name -> {
            if (Reserved.THIS.equals(name.first())) {
                final Name next = name.withoutFirst();
                return ColumnResolver.nestedColumn(input, next);
            } else {
                return ColumnResolver.nestedColumn(linkInput, name);
            }
        }));

        final Dataset<Tuple2<Row, Row>> joined = input.joinWith(linkInput, joinCondition, "left_outer");

        final SortTransform<Tuple2<Row, Row>> sortTransform = SortTransform.<Tuple2<Row, Row>>builder()
                .sort(link.getEffectiveSort())
                .columnResolver((d, name) -> ColumnResolver.nestedColumn(d.col("_2"), name))
                .build();

        final Dataset<Tuple2<Row, Row>> sorted = sortTransform.accept(joined);

        final MapFunction<Tuple2<Row, Row>, String> groupBy = v -> (String) SparkSchemaUtils.get(v._1(), Reserved.ID);
        final KeyValueGroupedDataset<String, Tuple2<Row, Row>> grouped = sorted.groupByKey(groupBy, Encoders.STRING());

        final MapGroupsFunction<String, Tuple2<Row, Row>, Row> collect = (id, iter) -> {

            Row root = null;
            final List<Row> values = new ArrayList<>();
            while (iter.hasNext()) {
                final Tuple2<Row, Row> tuple = iter.next();
                final Row _1 = tuple._1();
                final Row _2 = tuple._2();
                if (_2 != null) {
                    values.add(_2);
                }
                assert (root == null || root.equals(_1));
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

                return ids.stream().map(id -> SparkSchemaUtils.append(row, refIdField, id)).iterator();
            };

            final StructType flatMapType = SparkSchemaUtils.append(input.schema(), refIdField);
            final Dataset<Row> withRefIds = input.flatMap(refIds, RowEncoder.apply(flatMapType));

            final Column joinCondition = refInput.col(Reserved.ID).equalTo(withRefIds.col(REF_ID_COLUMN));
            final Dataset<Tuple2<Row, Row>> joined = withRefIds.joinWith(refInput, joinCondition, "left_outer");

            final MapFunction<Tuple2<Row, Row>, String> groupBy = v -> (String) SparkSchemaUtils.get(v._1(), Reserved.ID);
            final KeyValueGroupedDataset<String, Tuple2<Row, Row>> grouped = joined.groupByKey(groupBy, Encoders.STRING());

            final MapGroupsFunction<String, Tuple2<Row, Row>, Row> collect = (id, iter) -> {

                Row root = null;
                final Map<String, Row> lookup = new HashMap<>();
                while (iter.hasNext()) {
                    final Tuple2<Row, Row> tuple = iter.next();
                    final Row _1 = SparkSchemaUtils.remove(tuple._1(), REF_ID_COLUMN);
                    final Row _2 = tuple._2();
                    if (_2 != null) {
                        lookup.put((String) SparkSchemaUtils.get(_2, Reserved.ID), _2);
                    }
                    assert (root == null || root.equals(_1));
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

            output = grouped.mapGroups(collect, RowEncoder.apply(input.schema()));
        }

        return output;
    }

    protected Dataset<Row> cache(final Dataset<Row> ds) {

        return ds.cache();
    }

    @Data
    private static class RequiredRef {

        private final InstanceSchema schema;

        private final Set<Name> expand;
    }

    private Set<RequiredRef> requiredRefs(final Use<?> type, final Set<Name> expand) {

        return type.visit(new Use.Visitor.Defaulting<Set<RequiredRef>>() {

            @Override
            public Set<RequiredRef> visitDefault(final Use<?> type) {

                return ImmutableSet.of();
            }

            @Override
            public <T> Set<RequiredRef> visitCollection(final UseCollection<T, ? extends Collection<T>> type) {

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
            public Set<RequiredRef> visitInstance(final UseInstance type) {

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
                public Set<String> visitDefault(final Use<?> type) {

                    return Collections.emptySet();
                }

                @Override
                public <T> Set<String> visitCollection(final UseCollection<T, ? extends Collection<T>> type) {

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
                public Set<String> visitObject(final UseObject type) {

                    if (input instanceof Row) {
                        final Row row = (Row) input;
                        final String id = (String) SparkSchemaUtils.get(row, Reserved.ID);
                        return id == null ? ImmutableSet.of() : ImmutableSet.of(id);
                    } else {
                        throw new IllegalStateException();
                    }
                }
            });
        }
    }

    private Object applyRefs(final Use<?> type, final Set<Name> expand, final Object input, final Map<String, Row> lookup) {

        if (input == null) {
            return null;
        } else {
            return type.visit(new Use.Visitor.Defaulting<Object>() {

                @Override
                public Object visitDefault(final Use<?> type) {

                    return input;
                }

                @Override
                public <T> scala.collection.Iterable<?> visitCollection(final UseCollection<T, ? extends Collection<T>> type) {

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
                public Row visitObject(final UseObject type) {

                    if (input instanceof Row) {
                        final Row row = (Row) input;
                        final String id = (String) SparkSchemaUtils.get(row, Reserved.ID);
                        if (id != null) {
                            return lookup.get(id);
                        } else {
                            return null;
                        }
                    } else {
                        throw new IllegalStateException();
                    }
                }
            });
        }
    }
}
