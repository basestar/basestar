package io.basestar.spark.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Expression;
import io.basestar.expression.constant.Constant;
import io.basestar.schema.*;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.use.*;
import io.basestar.schema.util.Expander;
import io.basestar.spark.expression.SparkExpressionVisitor;
import io.basestar.spark.transform.Transform;
import io.basestar.spark.util.ScalaUtils;
import io.basestar.spark.util.SparkSchemaUtils;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Page;
import io.basestar.util.Sort;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ExpandStepTransform implements Transform<Dataset<Row>, Dataset<Row>> {

    private static final String REF_KEY = Reserved.PREFIX + "ref";

    private final LinkableSchema schema;

    private final QueryResolver resolver;

    private final Set<Name> expand;

    @lombok.Builder(builderClassName = "Builder")
    protected ExpandStepTransform(final LinkableSchema schema, final QueryResolver resolver, final Set<Name> expand) {

        this.schema = schema;
        this.resolver = resolver;
        this.expand = Immutable.copy(expand);
    }

    protected static String refKey(final ReferableSchema schema) {

        return REF_KEY + schema.getQualifiedName().toString(Reserved.PREFIX);
    }

    @Override
    public Dataset<Row> accept(final Dataset<Row> input) {

        return accept(input, schema, resolver, stripEmpty(expand));
    }

//    private static Dataset<Row> joinRef(final Dataset<Row> joined, final ReferableSchema refSchema, final Set<Name> refExpand, final QueryResolver resolver) {
//
//        final String refKey = refKey(refSchema);
//        final String resolvedKey = resolvedRefKey(refSchema);
//        final Dataset<Row> joinTo = resolver.resolve(refSchema, Constant.TRUE, ImmutableList.of(), refExpand).result();
//
//        final StructField resolvedKeyField = SparkSchemaUtils.field(resolvedKey, joinTo.schema());
//        final StructType joinedStructType = SparkSchemaUtils.append(joined.schema(), resolvedKeyField);
//
//        final Column joinColumn = joined.col(refKey).equalTo(joinTo.col(ReferableSchema.ID));
//
//        return joined.joinWith(joinTo, joinColumn, "left_outer")
//                .map((MapFunction<Tuple2<Row, Row>, Row>) v -> SparkSchemaUtils.append(v._1(), resolvedKeyField, v._2()), RowEncoder.apply(joinedStructType));
//    }
//
//    private static Dataset<Row> joinLink(final Dataset<Row> joined, final LinkableSchema schema, final Link link, final Set<Name> linkExpand, final QueryResolver resolver) {
//
//        final LinkableSchema linkSchema = link.getSchema();
//        final Expression linkExpression = link.getExpression();
//        final String resolvedKey = resolvedLinkKey(link);
//        // TODO: extract constant link expression part
//        final Dataset<Row> joinTo = resolver.resolve(linkSchema, Constant.TRUE, link.getEffectiveSort(), linkExpand).result();
//
//        final InferenceContext inferenceContext = InferenceContext.from(linkSchema)
//                .overlay(Reserved.THIS, InferenceContext.from(schema));
//
//        final Function<Name, Column> columnResolver = columnResolver(joined, joinTo);
//
//        final StructField resolvedKeyField = SparkSchemaUtils.field(resolvedKey, joinTo.schema());
//        final StructType joinedStructType = SparkSchemaUtils.append(joined.schema(), resolvedKeyField);
//
//        final Column joinColumn = new SparkExpressionVisitor(columnResolver, inferenceContext)
//                .visit(linkExpression);
//
//        return joined.joinWith(joinTo, joinColumn, "left_outer")
//                .map((MapFunction<Tuple2<Row, Row>, Row>) v -> SparkSchemaUtils.append(v._1(), resolvedKeyField, v._2()), RowEncoder.apply(joinedStructType));
//    }

//    @SuppressWarnings("unchecked")
//    private static Dataset<Row> accept(final Dataset<Row> input, final LinkableSchema schema, final QueryResolver resolver, final Set<Name> expand) {
//
//        return expand(input, schema, resolver, expand);
//
//        final Map<ReferableSchema, Set<Name>> refJoins = new HashMap<>();
//        final Map<Link, Set<Name>> linkJoins = new HashMap<>();
//
//        schema.expand(new Expander() {
//            @Override
//            public Instance expandRef(final Name name, final ReferableSchema schema, final Instance ref, final Set<Name> expand) {
//
//                refJoins.compute(schema, (k, v) -> Immutable.copyAddAll(v, expand));
//                return ref;
//            }
//
//            @Override
//            public Instance expandVersionedRef(final Name name, final ReferableSchema schema, final Instance ref, final Set<Name> expand) {
//
//                return ref;
//            }
//
//            @Override
//            public Page<Instance> expandLink(final Name name, final Link link, final Page<Instance> value, final Set<Name> expand) {
//
//                linkJoins.put(link, expand);
//                return value;
//            }
//        }, expand);
//
//        Dataset<Row> joined = input;
//
//        // Links must be joined first
//        for(final Map.Entry<Link, Set<Name>> entry : linkJoins.entrySet()) {
//            joined = joinLink(joined, schema, entry.getKey(), entry.getValue(), resolver);
//        }
//
//        for(final Map.Entry<ReferableSchema, Set<Name>> entry : refJoins.entrySet()) {
//            joined = joinRef(joined, entry.getKey(), entry.getValue(), resolver);
//        }
//
//        final StructType outputStructType = SparkSchemaUtils.structType(schema, expand);
//        final Encoder<Row> outputEncoder = RowEncoder.apply(outputStructType);
//
//        final String nameOfId = schema.id();
//        return joined.groupByKey(
//                (MapFunction<Row, Object>) row -> SparkSchemaUtils.get(row, nameOfId),
//                (Encoder<Object>)SparkSchemaUtils.encoder(schema.typeOfId())
//        ).mapGroups((MapGroupsFunction<Object, Row, Row>) (key, iterator) -> {
//
//            final Map<String, Object> data = new HashMap<>();
//            final Map<String, Instance> refs = new HashMap<>();
//            final Map<String, List<Instance>> links = new HashMap<>();
//            while(iterator.hasNext()) {
//                final Row row = iterator.next();
//                if(Nullsafe.orDefault((Boolean)SparkSchemaUtils.get(row, rootKey()))) {
//                    data.putAll(SparkSchemaUtils.fromSpark(schema, row));
//                    for (final Map.Entry<Link, Set<Name>> entry : linkJoins.entrySet()) {
//                        final Link link = entry.getKey();
//                        final String resolvedKey = resolvedLinkKey(link);
//                        final Row unpacked = (Row) SparkSchemaUtils.get(row, resolvedKey);
//                        if (unpacked != null) {
//                            final LinkableSchema linkSchema = link.getSchema();
//                            final Set<Name> linkExpand = entry.getValue();
//                            final Map<String, Object> resolved = SparkSchemaUtils.fromSpark(linkSchema, linkExpand, unpacked);
//                            links.computeIfAbsent(resolvedKey, ignored -> new ArrayList<>()).add(new Instance(resolved));
//                        }
//                    }
//                } else {
//                    for (final Map.Entry<ReferableSchema, Set<Name>> entry : refJoins.entrySet()) {
//                        final ReferableSchema refSchema = entry.getKey();
//                        final Set<Name> refExpand = entry.getValue();
//                        final String resolvedKey = resolvedRefKey(refSchema);
//                        final Row unpacked = (Row) SparkSchemaUtils.get(row, resolvedKey);
//                        if (unpacked != null) {
//                            final Map<String, Object> resolved = SparkSchemaUtils.fromSpark(refSchema, refExpand, unpacked);
//                            refs.put(refIdKey(refSchema, Instance.getId(resolved)), new Instance(resolved));
//                        }
//                    }
//                }
//            }
//
//            final Map<String, Object> result = schema.expand(new Instance(data), new Expander() {
//                @Override
//                public Instance expandRef(final Name name, final ReferableSchema schema, final Instance ref, final Set<Name> expand) {
//
//                    return Nullsafe.orDefault(refs.get(refIdKey(schema, Instance.getId(ref))), ref);
//                }
//
//                @Override
//                public Instance expandVersionedRef(final Name name, final ReferableSchema schema, final Instance ref, final Set<Name> expand) {
//
//                    return ref;
//                }
//
//                @Override
//                public Page<Instance> expandLink(final Name name, final Link link, final Page<Instance> value, final Set<Name> expand) {
//
//                    return Nullsafe.mapOrDefault(links.get(resolvedLinkKey(link)), Page::from, Page.empty());
//                }
//            }, expand);
//
//            return SparkSchemaUtils.toSpark(schema, expand, outputStructType, result);
//
//        }, outputEncoder);
//    }

    private static Function<Name, Column> columnResolver(final Dataset<Row> left, final Dataset<Row> right) {

        return name -> {
            if(name.get(0).equals(Reserved.THIS)) {
                final Name rest = name.withoutFirst();
                if(rest.isEmpty()) {
                    return functions.struct(left.col(ObjectSchema.ID));
                } else {
                    return next(left.col(rest.get(0)), rest.withoutFirst());
                }
            } else {
                return next(right.col(name.get(0)), name.withoutFirst());
            }
        };
    }

    private static Column next(final Column col, final Name rest) {

        if(rest.isEmpty()) {
            return col;
        } else {
            return next(col.getField(rest.first()), rest.withoutFirst());
        }
    }

    @SuppressWarnings("unchecked")
    private static Dataset<Row> accept(final Dataset<Row> input, final LinkableSchema schema, final QueryResolver resolver, final Set<Name> expand) {

        if(expand.isEmpty()) {
            return input;
        } else {

            final Map<String, Set<Name>> branches = Name.branch(expand);

            Dataset<Row> output = input;
            final StructType outputType = SparkSchemaUtils.structType(schema, expand);
            final Encoder<Row> outputEncoder = RowEncoder.apply(outputType);

            final Encoder<Object> keyEncoder = (Encoder<Object>)SparkSchemaUtils.encoder(schema.typeOfId());

            final StructField refField = SparkSchemaUtils.field(REF_KEY, DataTypes.StringType.asNullable());
            final StructType withRefIdType = SparkSchemaUtils.append(outputType, refField);
            final Encoder<Row> withRefIdEncoder = RowEncoder.apply(withRefIdType);

            final List<Map.Entry<ReferableSchema, Map<Name, Set<Name>>>> refEntries = new ArrayList<>(refs(schema, expand).entrySet());

            for(int i = 0; i != refEntries.size(); ++i) {

                final Map.Entry<ReferableSchema, Map<Name, Set<Name>>> entry = refEntries.get(i);
                final ReferableSchema refSchema = entry.getKey();
                final Set<Name> refExpand = stripEmpty(flatten(entry.getValue().values()));

                if(i == 0) {
                    final Set<Name> names = entry.getValue().keySet();
                    final Map<String, Set<Name>> nameBranches = Name.branch(names);
                    output = output.flatMap(
                            (FlatMapFunction<Row, Row>)row -> {
                                final Row conformed = SparkSchemaUtils.conform(row, withRefIdType);
                                return withRefIds(schema, nameBranches, conformed);
                            },
                            withRefIdEncoder
                    );
                }
                final Dataset<Row> joinFrom = output;

                final StructType joinType = SparkSchemaUtils.structType(refSchema, refExpand);
                final Dataset<Row> joinTo = conform(resolver.resolve(refSchema, Constant.TRUE, ImmutableList.of(), refExpand).result(), joinType);

                final Column condition = joinFrom.col(REF_KEY).equalTo(joinTo.col(ReferableSchema.ID));
                final Dataset<Tuple2<Row, Row>> joined = joinFrom.joinWith(joinTo, condition, "left_outer");

                final KeyValueGroupedDataset<Object, Tuple2<Row, Row>> grouped = joined.groupByKey((MapFunction<Tuple2<Row, Row>, Object>)(tuple -> SparkSchemaUtils.get(tuple._1(), schema.id())), keyEncoder);

                if(i + 1 != refEntries.size()) {
                    final Map.Entry<ReferableSchema, Map<Name, Set<Name>>> nextEntry = refEntries.get(i + 1);
                    final Set<Name> nextNames = nextEntry.getValue().keySet();

                    final Map<String, Set<Name>> nextNameBranches = Name.branch(nextNames);
                    output = grouped.flatMapGroups((FlatMapGroupsFunction<Object, Tuple2<Row, Row>, Row>)(ignored, tuples) -> {

                        final Row resolved = applyRefs(schema, branches, tuples);
                        return withRefIds(schema, nextNameBranches, resolved);

                    }, withRefIdEncoder);
                } else {
                    output = grouped.mapGroups(
                            (MapGroupsFunction<Object, Tuple2<Row, Row>, Row>)(ignored, tuples) -> {

                                final Row result = applyRefs(schema, branches, tuples);
                                return SparkSchemaUtils.conform(result, outputType);
                            },
                            outputEncoder
                    );
                }
            }

            if(refEntries.isEmpty()) {
                output = conform(output, outputType);
            }

            for(final Map.Entry<Link, Set<Name>> entry : links(schema, expand).entrySet()) {
                final Link link = entry.getKey();
                final Expression linkExpression = link.getExpression();
                final LinkableSchema linkSchema = link.getSchema();
                final Set<Name> linkExpand = stripEmpty(entry.getValue());

                final StructType joinType = SparkSchemaUtils.structType(linkSchema, linkExpand);
                final Dataset<Row> joinTo = conform(resolver.resolve(linkSchema, Constant.TRUE, ImmutableList.of(), linkExpand).result(), joinType);

                final InferenceContext inferenceContext = InferenceContext.from(linkSchema)
                        .overlay(Reserved.THIS, InferenceContext.from(schema));

                final Function<Name, Column> columnResolver = columnResolver(output, joinTo);
                final Column condition = new SparkExpressionVisitor(columnResolver, inferenceContext)
                        .visit(linkExpression);

                final Dataset<Tuple2<Row, Row>> joined = output.joinWith(joinTo, condition, "left_outer");

                output = joined.groupByKey((MapFunction<Tuple2<Row, Row>, Object>)(tuple -> SparkSchemaUtils.get(tuple._1(), schema.id())), keyEncoder)
                        .mapGroups(applyLink(link, outputType), outputEncoder);
            }

            return output;
        }
    }

    private static Dataset<Row> conform(final Dataset<Row> input, final StructType type) {

        if(input.schema().equals(type)) {
            return input;
        } else {
            return input.map(
                    (MapFunction<Row, Row>)row -> SparkSchemaUtils.conform(row, type),
                    RowEncoder.apply(type)
            );
        }
    }

    private static Set<Name> stripEmpty(final Set<Name> expand) {

        return expand.stream().filter(v -> !v.isEmpty()).collect(Collectors.toSet());
    }

    private static Set<Name> flatten(final Collection<Set<Name>> value) {

        return value.stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

    private static Map<ReferableSchema, Map<Name, Set<Name>>> refs(final LinkableSchema schema, final Set<Name> expand) {

        final Map<ReferableSchema, Map<Name, Set<Name>>> schemas = new TreeMap<>(Comparator.comparing(ReferableSchema::getQualifiedName));
        schema.expand(new Expander.Noop() {
            @Override
            public Instance expandRef(final Name name, final ReferableSchema schema, final Instance ref, final Set<Name> expand) {

                schemas.compute(schema, (k, v) -> Immutable.copyPut(v, name, expand));
                return super.expandRef(name, schema, ref, expand);
            }
        }, expand);
        return schemas;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static MapGroupsFunction<Object, Tuple2<Row, Row>, Row> applyLink(final Link link, final StructType outputType) {

        return (key, tuples) -> {

            Row root = null;
            final List<Row> page = new ArrayList<>();

            while(tuples.hasNext()) {
                final Tuple2<Row, Row> tuple = tuples.next();

                if(root == null) {
                    root = tuple._1();
                }
                if(tuple._2() != null) {
                    page.add(tuple._2());
                }
            }

            assert root != null;

            // Need to handle scala -> java conversion here
            page.sort(Sort.comparator(link.getEffectiveSort(), (r, n) -> (Comparable)SparkSchemaUtils.get(r, n)));
            final Object value;
            if(link.isSingle()) {
                value = page.isEmpty() ? null : page.get(0);
            } else {
                value = ScalaUtils.asScalaSeq(page);
            }
            return SparkSchemaUtils.set(root, link.getName(), value);
        };
    }

    private static Map<Link, Set<Name>> links(final LinkableSchema schema, final Set<Name> expand) {

        final Map<Link, Set<Name>> links = new TreeMap<>(Comparator.comparing(Link::getQualifiedName));
        schema.expand(new Expander.Noop() {

            @Override
            public Page<Instance> expandLink(final Name name, final Link link, final Page<Instance> value, final Set<Name> expand) {

                links.put(link, expand);
                return super.expandLink(name, link, value, expand);
            }
        }, expand);
        return links;
    }

    private static Iterator<Row> withRefIds(final LinkableSchema schema, final Map<String, Set<Name>> branches, final Row row) {

        final Set<String> refIds = new HashSet<>();
        schema.getProperties().forEach((name, property) -> {
            final Set<Name> branch = branches.get(name);
            if(branch != null) {
                refIds.addAll(refIds(property.getType(), branch, SparkSchemaUtils.get(row, name)));
            }
        });

        final List<Row> result = new ArrayList<>();
        for(final String refId : refIds) {
            result.add(SparkSchemaUtils.set(row, REF_KEY, refId));
        }
        if(result.isEmpty()) {
            result.add(SparkSchemaUtils.set(row, REF_KEY, null));
        }

        return result.iterator();
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

    private static Row applyRefs(final LinkableSchema schema, final Map<String, Set<Name>> branches, final Iterator<Tuple2<Row, Row>> tuples) {

        Row root = null;
        final Map<String, Row> refs = new HashMap<>();

        while(tuples.hasNext()) {
            final Tuple2<Row, Row> tuple = tuples.next();

            if(root == null) {
                root = tuple._1();
            }
            if(tuple._2() != null) {
                refs.put(SparkSchemaUtils.getId(tuple._2()), tuple._2());
            }
        }

        assert root != null;

        return SparkSchemaUtils.transform(root, (field, oldValue) -> {
            final String name = field.name();
            final Set<Name> branch = branches.get(name);
            if(branch != null) {
                final Property property = schema.getProperty(name, true);
                if(property != null) {
                    return applyRefs(property.getType(), branch, oldValue, refs);
                }
            }
            return oldValue;
        });
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
