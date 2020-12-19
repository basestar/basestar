package io.basestar.spark.expand;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.constant.Constant;
import io.basestar.schema.*;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.expression.InferenceVisitor;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseCollection;
import io.basestar.schema.use.UseInstance;
import io.basestar.schema.use.UseMap;
import io.basestar.spark.expression.SparkExpressionVisitor;
import io.basestar.spark.query.LinkExpressionVisitor;
import io.basestar.spark.query.QueryResolver;
import io.basestar.spark.util.ScalaUtils;
import io.basestar.spark.util.SparkRowUtils;
import io.basestar.spark.util.SparkSchemaUtils;
import io.basestar.util.Name;
import io.basestar.util.Sort;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.*;
import scala.Tuple2;
import scala.collection.Seq;

import java.util.*;
import java.util.function.Function;

@Slf4j
@Getter
public class ExpandLinkStep extends AbstractExpandStep {

    private final ExpandStep next;

    private final LinkableSchema root;

    private final LinkableSchema source;

    private final Link link;

    private final Name name;

    private final Expression closedExpression;

    private final SortedMap<String, Expression> constants;

    private final SortedMap<String, Use<?>> keyTypes;

    public ExpandLinkStep(final ExpandStep next, final LinkableSchema root, final LinkableSchema source, final Link link, final Name name) {

        this.next = next;
        this.root = root;
        this.source = source;
        this.link = link;
        this.name = name;

        final LinkExpressionVisitor closeLeft = new LinkExpressionVisitor(Reserved.PREFIX + "l_", ImmutableSet.of(Reserved.THIS));
        final Expression closedLeft = closeLeft.visit(link.getExpression());
        this.constants = new TreeMap<>(closeLeft.getConstants());
        if(constants.isEmpty()) {
            throw new IllegalStateException("Link expression must have constants on left side");
        }
        this.closedExpression = closedLeft;

        final InferenceContext inferenceContext = InferenceContext.empty()
                .overlay(Reserved.THIS, InferenceContext.from(source));
        final InferenceVisitor inference = new InferenceVisitor(inferenceContext);

        this.keyTypes = new TreeMap<>();
        for(final Map.Entry<String, Expression> entry : constants.entrySet()) {
            keyTypes.put(entry.getKey(), inference.visit(entry.getValue()));
        }
    }

    @Override
    protected String describe() {

        return "Expand link for " + root.getQualifiedName() + "(" + name + ")";
    }

    @Override
    protected boolean hasProjectedKeys(final StructType schema) {

        return SparkRowUtils.findField(schema, constants.keySet().iterator().next()).isPresent();
    }

    @Override
    public StructType projectKeysType(final StructType inputType) {

        StructType outputType = inputType;
        for(final Map.Entry<String, Use<?>> entry : keyTypes.entrySet()) {
            outputType = SparkRowUtils.append(outputType, SparkSchemaUtils.field(entry.getKey(), entry.getValue(), ImmutableSet.of()));
        }
        return outputType;
    }

    @Override
    public Column[] projectedKeyColumns() {

        return constants.keySet().stream()
                .map(functions::col).toArray(Column[]::new);
    }

    private DataType linkKeyType(final InstanceSchema schema, final Name name, final StructType type) {

        final Member member = schema.requireMember(name.first(), true);
        if(member instanceof Link && name.size() == 1) {
            return type;
        } else {
            final StructField field = SparkRowUtils.findField(type, name.first()).orElseThrow(IllegalStateException::new);
            return linkKeyType(member.getType(), name.withoutFirst(), field.dataType());
        }
    }

    private DataType linkKeyType(final Use<?> type, final Name name, final DataType dataType) {

        return type.visit(new Use.Visitor.Defaulting<DataType>() {

            @Override
            public <V, T extends Collection<V>> DataType visitCollection(final UseCollection<V, T> type) {

                if (dataType instanceof ArrayType) {
                    return linkKeyType(type.getType(), name, ((ArrayType) dataType).elementType());
                } else {
                    throw new IllegalStateException();
                }
            }

            @Override
            public <T> DataType visitMap(final UseMap<T> type) {

                if (dataType instanceof MapType) {
                    return linkKeyType(type.getType(), name.withoutFirst(), ((MapType) dataType).valueType());
                } else {
                    throw new IllegalStateException();
                }
            }

            @Override
            public DataType visitInstance(final UseInstance type) {

                if (dataType instanceof StructType) {
                    return linkKeyType(type.getSchema(), name, (StructType) dataType);
                } else {
                    throw new IllegalStateException();
                }
            }
        });
    }

    @Override
    public Iterator<Row> projectKeys(final StructType outputType, final Row row) {

        final Set<Row> keys = linkKeys(root, name, row);

        final List<Row> result = new ArrayList<>();
        for(final Row key : keys) {
            // TODO: context that doesn't require row -> instance conversion
            final Map<String, Object> instance = SparkSchemaUtils.fromSpark(source, key);
            final Context context = Context.init(ImmutableMap.of(Reserved.THIS, instance));
            Row merged = row;
            for(final Map.Entry<String, Expression> entry : constants.entrySet()) {
                final StructField field = SparkRowUtils.getField(outputType, entry.getKey());
                final Object value = entry.getValue().evaluate(context);
                merged = SparkRowUtils.append(merged, field, value);
            }
            result.add(merged);
        }
        if(result.isEmpty()) {
            Row merged = row;
            for(final Map.Entry<String, Expression> entry : constants.entrySet()) {
                final StructField field = SparkRowUtils.getField(outputType, entry.getKey());
                merged = SparkRowUtils.append(merged, field, null);
            }
            result.add(merged);
        }

        return result.iterator();
    }

    @Override
    protected <T> Dataset<Row> applyImpl(final QueryResolver resolver, final Dataset<Row> input, final Use<T> typeOfId) {

        final int inputPartitions = input.rdd().partitions().length;

        final LinkableSchema linkSchema = link.getSchema();

        final Dataset<Row> joinTo = resolver.resolve(linkSchema, Constant.TRUE, ImmutableList.of(), ImmutableSet.of())
                .result();

        log.warn("{} has {} source partitions", describe(), joinTo.rdd().partitions().length);

        final StructType joinToType = joinTo.schema();
        final Column[] groupColumns = projectedKeyColumns();
        // Input is already partitioned by the key columns by chain fusing
        final RelationalGroupedDataset groupedInput = input.groupBy(groupColumns);

        final Dataset<Row> collectedInput = groupedInput
                .agg(functions.collect_list(functions.struct(functions.col("*"))).as("_rows"));

        final Function<Name, Column> columnResolver = columnResolver(collectedInput, joinTo);
        final InferenceContext inferenceContext = InferenceContext.from(linkSchema)
                .with(keyTypes);
        final Column condition = new SparkExpressionVisitor(columnResolver, inferenceContext)
                .visit(closedExpression);

        log.warn("{} has {} collected input partitions", describe(), collectedInput.rdd().partitions().length);

        final Dataset<Tuple2<Row, Row>> groupJoined = collectedInput.joinWith(joinTo, condition, "left_outer");

        final int seqIndex = groupColumns.length;
        final Dataset<Tuple2<Row, Row>> joined = groupJoined.flatMap(
                (FlatMapFunction<Tuple2<Row, Row>, Tuple2<Row, Row>>) tuple -> {

                    final Seq<Row> left = tuple._1().getSeq(seqIndex);
                    return ScalaUtils.asJavaStream(left)
                            .map(l -> Tuple2.apply(l, tuple._2()))
                            .iterator();
                },
                Encoders.tuple(RowEncoder.apply(input.schema()), RowEncoder.apply(joinTo.schema()))
        );

        log.warn("{} has {} join partitions", describe(), joined.rdd().partitions().length);

        final DataType linkType;
        if (link.isSingle()) {
            linkType = joinToType;
        } else {
            linkType = DataTypes.createArrayType(joinToType);
        }

        final KeyValueGroupedDataset<T, Tuple2<Row, Row>> grouped = groupResults(joined);

        final StructType outputType = expandedType(root, ImmutableSet.of(name), cleanKeys(input.schema()), linkType);

        if (next != null) {

            // Fuse the initial flat map part of the next step

            final StructType projectedType = next.projectKeysType(outputType);
            return next.apply(resolver, grouped.flatMapGroups((FlatMapGroupsFunction<T, Tuple2<Row, Row>, Row>) (ignored, tuples) -> {

                final Row resolved = applyLink(root, name, tuples);
                final Row clean = cleanKeys(resolved);
                return next.projectKeys(projectedType, clean);

            }, RowEncoder.apply(projectedType)));

        } else {

            return grouped.mapGroups(
                    (MapGroupsFunction<T, Tuple2<Row, Row>, Row>) (ignored, tuples) -> {

                        final Row resolved = applyLink(root, name, tuples);
                        return cleanKeys(resolved);
                    },
                    RowEncoder.apply(outputType)
            );
        }
    }

    private Row cleanKeys(final Row row) {

        Row clean = row;
        for(final Map.Entry<String, Expression> entry : constants.entrySet()) {
            clean = SparkRowUtils.remove(clean, entry.getKey());
        }
        return clean;
    }

    private StructType cleanKeys(final StructType type) {

        StructType clean = type;
        for(final Map.Entry<String, Expression> entry : constants.entrySet()) {
            clean = SparkRowUtils.remove(clean, entry.getKey());
        }
        return clean;
    }

    private Function<Name, Column> columnResolver(final Dataset<Row> left, final Dataset<Row> right) {

        return name -> {
            final String first = name.first();
            if(constants.containsKey(first)) {
                return next(left.col(first), name.withoutFirst());
            } else {
                return next(right.col(first), name.withoutFirst());
//                if(rightConstants.containsKey(first)) {
//                    return next(right.col(first), name.withoutFirst());
//                } else {
//                    throw new IllegalStateException("Name " + name + " must be constant on at least one side of the link");
//                }
            }
        };
    }

    private static Column next(final Column col, final Name rest) {

        if (rest.isEmpty()) {
            return col;
        } else {
            return next(col.getField(rest.first()), rest.withoutFirst());
        }
    }

    private static Set<Row> linkKeys(final InstanceSchema root, final Name name, final Row row) {

        final Set<Row> refKeys = new HashSet<>();
        final Member member = root.getMember(name.first(), true);
        if (member != null) {
            if (member instanceof Link && name.size() == 1) {
                refKeys.add(row);
            } else {
                refKeys.addAll(linkKeys(member.getType(), name.withoutFirst(), SparkRowUtils.get(row, name)));
            }
        }
        return refKeys;
    }

    private static Set<Row> linkKeys(final Use<?> type, final Name name, final Object input) {

        if (input == null) {
            return Collections.emptySet();
        } else {

            return type.visit(new Use.Visitor.Defaulting<Set<Row>>() {

                @Override
                public <T> Set<Row> visitDefault(final Use<T> type) {

                    return Collections.emptySet();
                }

                @Override
                public <V, T extends Collection<V>> Set<Row> visitCollection(final UseCollection<V, T> type) {

                    if (input instanceof scala.collection.Iterable<?>) {
                        final Set<Row> results = new HashSet<>();
                        ((scala.collection.Iterable<?>) input)
                                .foreach(ScalaUtils.scalaFunction(v -> results.addAll(linkKeys(type.getType(), name, v))));
                        return results;
                    } else {
                        throw new IllegalStateException();
                    }
                }

                @Override
                public <T> Set<Row> visitMap(final UseMap<T> type) {

                    if (input instanceof scala.collection.Map<?, ?>) {
                        final Set<Row> results = new HashSet<>();
                        if(!name.isEmpty() && name.first().equals(UseMap.EXPAND_WILDCARD)) {
                            ((scala.collection.Map<?, ?>) input)
                                    .foreach(ScalaUtils.scalaFunction(t -> results.addAll(linkKeys(type.getType(), name.withoutFirst(), t._2()))));
                        }
                        return results;
                    } else {
                        throw new IllegalStateException();
                    }
                }

                @Override
                public Set<Row> visitInstance(final UseInstance type) {

                    if (input instanceof Row) {
                        return linkKeys(type.getSchema(), name, (Row)input);
                    } else {
                        throw new IllegalStateException();
                    }
                }
            });
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Row applyLink(final LinkableSchema schema, final Name name, final Iterator<Tuple2<Row, Row>> tuples) {

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
        page.sort(Sort.comparator(link.getEffectiveSort(), (r, n) -> (Comparable) SparkRowUtils.get(r, n)));
        final Object value;
        if(link.isSingle()) {
            value = page.isEmpty() ? null : page.get(0);
        } else {
            value = ScalaUtils.asScalaSeq(page);
        }

        return applyLink(schema, name, root, value);
    }

    private static Row applyLink(final InstanceSchema schema, final Name name, final Row input, final Object value) {

        return SparkRowUtils.transform(input, (field, oldValue) -> {

            if(field.name().equals(name.first())) {
                final Member member = schema.getMember(name.first(), true);
                if(member != null) {
                    if(member instanceof Link && name.size() == 1) {
                        return value;
                    } else {
                        return applyLink(member.getType(), name.withoutFirst(), oldValue, value);
                    }
                }
            }
            return oldValue;
        });
    }

    private static Object applyLink(final Use<?> type, final Name name, final Object input, final Object value) {

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
                                .foreach(ScalaUtils.scalaFunction(v -> results.add(applyLink(type.getType(), name, v, value))));
                        return ScalaUtils.asScalaSeq(results);
                    } else {
                        throw new IllegalStateException();
                    }
                }

                @Override
                public <T> scala.collection.Map<?, ?> visitMap(final UseMap<T> type) {

                    if (input instanceof scala.collection.Map<?, ?>) {
                        final Map<String, Object> results = new HashMap<>();
                        if(!name.isEmpty() && name.first().equals(UseMap.EXPAND_WILDCARD)) {
                            ((scala.collection.Map<?, ?>) input)
                                    .foreach(ScalaUtils.scalaFunction(t -> results.put((String)t._1(), applyLink(type.getType(), name.withoutFirst(), t._2(), value))));
                        }
                        return ScalaUtils.asScalaMap(results);
                    } else {
                        throw new IllegalStateException();
                    }
                }

                @Override
                public Row visitInstance(final UseInstance type) {

                    if (input instanceof Row) {
                        return applyLink(type.getSchema(), name, (Row)input, value);
                    } else {
                        throw new IllegalStateException();
                    }
                }
            });
        }
    }
}
