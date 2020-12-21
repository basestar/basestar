package io.basestar.spark.expand;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.basestar.expression.constant.Constant;
import io.basestar.schema.*;
import io.basestar.schema.use.*;
import io.basestar.spark.query.QueryResolver;
import io.basestar.spark.util.ScalaUtils;
import io.basestar.spark.util.SparkRowUtils;
import io.basestar.spark.util.SparkSchemaUtils;
import io.basestar.util.Name;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.collection.Seq;

import java.util.*;

/**
 * Joining is an expensive operation so this code is highly optimized to reduce the amount of work done in map stages.
 *
 * Expand stages are designed so that the final stage of the previous step is fused to the first stage of the next step.
 */

@Slf4j
@Getter
@RequiredArgsConstructor
public class ExpandRefsStep extends AbstractExpandStep {

    private static final String KEY = Reserved.PREFIX + "expand_key";

    private final ExpandStep next;

    private final LinkableSchema root;

    private final ReferableSchema target;

    private final Set<Name> names;

    @Override
    public StructType projectKeysType(final StructType inputType) {

        return SparkRowUtils.append(inputType, SparkRowUtils.field(KEY, DataTypes.StringType));
    }

    @Override
    public Iterator<Row> projectKeys(final StructType outputType, final Row row) {

        final StructField field = SparkRowUtils.requireField(outputType, KEY);

        final Set<String> refIds = refKeys(root, names, row);

        final List<Row> result = new ArrayList<>();
        for(final String refId : refIds) {
            result.add(SparkRowUtils.append(row, field, refId));
        }
        if(result.isEmpty()) {
            result.add(SparkRowUtils.append(row, field, null));
        }

        return result.iterator();
    }

    @Override
    protected String describe() {

        return "Expand refs for " + root.getQualifiedName() + "(" + names + ")";
    }

    @Override
    public Column[] projectedKeyColumns() {

        return new Column[] { functions.col(KEY) };
    }

    @Override
    protected boolean hasProjectedKeys(final StructType schema) {

        return SparkRowUtils.findField(schema, KEY).isPresent();
    }

    protected <T> Dataset<Row> applyImpl(final QueryResolver resolver, final Dataset<Row> input, final Use<T> typeOfId) {

        final Dataset<Row> joinTo = resolver.resolve(target, Constant.TRUE, ImmutableList.of(), ImmutableSet.of()).dataset();

        final StructType joinToType = joinTo.schema();
        // Input is already partitioned by the key columns by chain fusing
        final RelationalGroupedDataset groupedInput = input.groupBy(functions.col(KEY));

        final Dataset<Row> collectedInput = groupedInput
            .agg(functions.collect_list(functions.struct(functions.col("*"))).as("_rows"));

        final Column condition = collectedInput.col(KEY).equalTo(joinTo.col(ReferableSchema.ID));

        final Dataset<Tuple2<Row, Row>> groupJoined = collectedInput.joinWith(joinTo, condition, "left_outer");

        final Dataset<Tuple2<Row, Row>> joined = groupJoined.flatMap(
                (FlatMapFunction<Tuple2<Row, Row>, Tuple2<Row, Row>>) tuple -> {

                    final Seq<Row> left = tuple._1().getSeq(1);
                    return ScalaUtils.asJavaStream(left)
                            .map(l -> Tuple2.apply(l, tuple._2()))
                            .iterator();
                },
                Encoders.tuple(RowEncoder.apply(input.schema()), RowEncoder.apply(joinTo.schema()))
        );

        final KeyValueGroupedDataset<T, Tuple2<Row, Row>> grouped = groupResults(joined);

        final StructType outputType = expandedType(root, names, SparkRowUtils.remove(input.schema(), KEY), joinToType);

        if (next != null) {

            // Fuse the initial flat map part of the next step

            final StructType projectedType = next.projectKeysType(outputType);
            return next.apply(resolver, grouped.flatMapGroups((FlatMapGroupsFunction<T, Tuple2<Row, Row>, Row>) (ignored, tuples) -> {

                final Row resolved = applyRefs(root, names, joinToType, tuples);
                // Remove the old key field
                final Row clean = SparkRowUtils.remove(resolved, KEY);
                return next.projectKeys(projectedType, clean);

            }, RowEncoder.apply(projectedType)));

        } else {

            return grouped.mapGroups(
                    (MapGroupsFunction<T, Tuple2<Row, Row>, Row>) (ignored, tuples) -> {

                        final Row resolved = applyRefs(root, names, joinToType, tuples);
                        return SparkRowUtils.remove(resolved, KEY);
                    },
                    RowEncoder.apply(outputType)
            );
        }
    }

    private static Set<String> refKeys(final InstanceSchema root, final Set<Name> expand, final Row row) {

        final Map<String, Set<Name>> branches = Name.branch(expand);

        final Set<String> refIds = new HashSet<>();
        root.getMembers().forEach((name, member) -> {
            final Set<Name> branch = branches.get(name);
            if(branch != null) {
                refIds.addAll(refKeys(member.typeOf(), branch, SparkRowUtils.get(row, name)));
            }
        });
        return refIds;
    }

    private static Set<String> refKeys(final Use<?> type, final Set<Name> expand, final Object input) {

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
                                .foreach(ScalaUtils.scalaFunction(v -> results.addAll(refKeys(type.getType(), expand, v))));
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
                                    .foreach(ScalaUtils.scalaFunction(t -> results.addAll(refKeys(type.getType(), branch, t._2()))));
                        }
                        return results;
                    } else {
                        throw new IllegalStateException();
                    }
                }

                @Override
                public Set<String> visitInstance(final UseInstance type) {

                    if (input instanceof Row) {
                        return refKeys(type.getSchema(), expand, (Row)input);
                    } else {
                        throw new IllegalStateException();
                    }
                }

                @Override
                public Set<String> visitRef(final UseRef type) {

                    if (input instanceof Row) {
                        if(expand.isEmpty()) {
                            final Row row = (Row) input;
                            final String id = (String) SparkRowUtils.get(row, ObjectSchema.ID);
                            return id == null ? ImmutableSet.of() : ImmutableSet.of(id);
                        } else {
                            return refKeys(type.getSchema(), expand, (Row)input);
                        }
                    } else {
                        throw new IllegalStateException();
                    }
                }
            });
        }
    }

    private static Row applyRefs(final LinkableSchema schema, final Set<Name> expand, final StructType joinType, final Iterator<Tuple2<Row, Row>> tuples) {

        Row root = null;
        final Map<String, Row> refs = new HashMap<>();

        while (tuples.hasNext()) {
            final Tuple2<Row, Row> tuple = tuples.next();

            if (root == null) {
                root = tuple._1();
            }
            if (tuple._2() != null) {
                refs.put(SparkSchemaUtils.getId(tuple._2()), tuple._2());
            }
        }

        assert root != null;

        return applyRefs(schema, expand, joinType, root, refs);
    }

    private static Row applyRefs(final InstanceSchema schema, final Set<Name> expand, final StructType joinType, final Row input, final Map<String, Row> lookup) {

        final Map<String, Set<Name>> branches = Name.branch(expand);
        return SparkRowUtils.transform(input, (field, oldValue) -> {
            final String name = field.name();
            final Set<Name> branch = branches.get(name);
            if(branch != null) {
                final Member member = schema.getMember(name, true);
                if(member != null) {
                    return applyRefs(member.typeOf(), branch, joinType, oldValue, lookup);
                }
            }
            return oldValue;
        });
    }

    private static Object applyRefs(final Use<?> type, final Set<Name> expand, final StructType joinType, final Object input, final Map<String, Row> lookup) {

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
                                .foreach(ScalaUtils.scalaFunction(v -> results.add(applyRefs(type.getType(), expand, joinType, v, lookup))));
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
                                    .foreach(ScalaUtils.scalaFunction(t -> results.put((String)t._1(), applyRefs(type.getType(), branch, joinType, t._2(), lookup))));
                        }
                        return ScalaUtils.asScalaMap(results);
                    } else {
                        throw new IllegalStateException();
                    }
                }

                @Override
                public Row visitInstance(final UseInstance type) {

                    if (input instanceof Row) {
                        return applyRefs(type.getSchema(), expand, joinType, (Row)input, lookup);
                    } else {
                        throw new IllegalStateException();
                    }
                }

                @Override
                public Row visitRef(final UseRef type) {

                    if (input instanceof Row) {
                        if(expand.isEmpty()) {
                            final Row row = (Row) input;
                            final String id = (String) SparkRowUtils.get(row, ObjectSchema.ID);
                            if (id != null && lookup.get(id) != null) {
                                return lookup.get(id);
                            } else {
                                // Must return a result that conforms to the new schema
                                final Object[] values = new Object[joinType.size()];
                                values[joinType.fieldIndex(ReferableSchema.ID)] = id;
                                return new GenericRowWithSchema(values, joinType);
                            }
                        } else {
                            return applyRefs(type.getSchema(), expand, joinType, (Row)input, lookup);
                        }
                    } else {
                        throw new IllegalStateException();
                    }
                }
            });
        }
    }
}
