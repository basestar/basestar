package io.basestar.spark.expand;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
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
import io.basestar.spark.expression.ClosureExtractingVisitor;
import io.basestar.spark.expression.SparkExpressionVisitor;
import io.basestar.spark.query.QueryResolver;
import io.basestar.spark.util.ScalaUtils;
import io.basestar.spark.util.SparkRowUtils;
import io.basestar.spark.util.SparkSchemaUtils;
import io.basestar.spark.util.SparkUtils;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Sort;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;
import java.util.function.Function;

@Slf4j
@Getter
public class ExpandLinkStep extends AbstractExpandStep {

    private static final String L_PREFIX = Reserved.PREFIX + "l_";

    private static final String R_PREFIX = Reserved.PREFIX + "r_";

    private final ExpandStep next;

    private final LinkableSchema root;

    private final LinkableSchema source;

    private final Link link;

    private final Name name;

    private final Expression closedExpression;

    private final SortedMap<String, Expression> leftConstants;

    private final SortedMap<String, Expression> rightConstants;

    private final SortedMap<String, Use<?>> leftKeyTypes;

    private final SortedMap<String, Use<?>> rightKeyTypes;

    public ExpandLinkStep(final ExpandStep next, final LinkableSchema root, final LinkableSchema source, final Link link, final Name name) {

        this.next = next;
        this.root = root;
        this.source = source;
        this.link = link;
        this.name = name;

        final ClosureExtractingVisitor closeLeft = new ClosureExtractingVisitor(L_PREFIX, Reserved.THIS::equals);
        final Expression closedLeft = closeLeft.visit(link.getExpression());
        this.leftConstants = Immutable.sortedMap(closeLeft.getConstants());
        if(leftConstants.isEmpty()) {
            throw new IllegalStateException("Link expression must have constants on left side");
        }

        final ClosureExtractingVisitor closeRight = new ClosureExtractingVisitor(R_PREFIX, n -> !Reserved.THIS.equals(n) && !n.startsWith(L_PREFIX));
        final Expression closedRight = closeRight.visit(closedLeft);
        this.rightConstants = Immutable.sortedMap(closeRight.getConstants());
        this.closedExpression = closedRight;

        final InferenceContext inferenceContext = InferenceContext.from(link.getSchema())
                .overlay(Reserved.THIS, InferenceContext.from(source));
        final InferenceVisitor inference = new InferenceVisitor(inferenceContext);

        this.leftKeyTypes = new TreeMap<>();
        for(final Map.Entry<String, Expression> entry : leftConstants.entrySet()) {
            leftKeyTypes.put(entry.getKey(), inference.visit(entry.getValue()));
        }
        this.rightKeyTypes = new TreeMap<>();
        for(final Map.Entry<String, Expression> entry : rightConstants.entrySet()) {
            rightKeyTypes.put(entry.getKey(), inference.visit(entry.getValue()));
        }
    }

    @Override
    protected String describe() {

        return "Expand link for " + root.getQualifiedName() + "(" + name + ")";
    }

    @Override
    protected boolean hasProjectedKeys(final StructType schema) {

        return SparkRowUtils.findField(schema, leftConstants.keySet().iterator().next()).isPresent();
    }

    @Override
    public StructType projectKeysType(final StructType inputType) {

        return projectKeysType(inputType, leftKeyTypes);
    }

    private static StructType projectKeysType(final StructType inputType, final Map<String, Use<?>> keyTypes) {

        StructType outputType = inputType;
        for(final Map.Entry<String, Use<?>> entry : keyTypes.entrySet()) {
            outputType = SparkRowUtils.append(outputType, SparkSchemaUtils.field(entry.getKey(), entry.getValue(), ImmutableSet.of()));
        }
        return outputType;
    }

    @Override
    public Column[] projectedKeyColumns() {

        return leftConstants.keySet().stream()
                .map(functions::col).toArray(Column[]::new);
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
            for(final Map.Entry<String, Expression> entry : leftConstants.entrySet()) {
                final StructField field = SparkRowUtils.requireField(outputType, entry.getKey());
                final Object value = entry.getValue().evaluate(context);
                merged = SparkRowUtils.append(merged, field, value);
            }
            result.add(merged);
        }
        if(result.isEmpty()) {
            Row merged = row;
            for(final Map.Entry<String, Expression> entry : leftConstants.entrySet()) {
                final StructField field = SparkRowUtils.requireField(outputType, entry.getKey());
                merged = SparkRowUtils.append(merged, field, null);
            }
            result.add(merged);
        }

        return result.iterator();
    }

    public Row projectRightKeys(final StructType outputType, final Row row) {

        final Map<String, Object> instance = SparkSchemaUtils.fromSpark(link.getSchema(), row);
        final Context context = Context.init(instance);
        Row merged = row;
        for(final Map.Entry<String, Expression> entry : rightConstants.entrySet()) {
            final StructField field = SparkRowUtils.requireField(outputType, entry.getKey());
            final Object value = entry.getValue().evaluate(context);
            merged = SparkRowUtils.append(merged, field, value);
        }
        return merged;
    }

    @Override
    protected <T> Dataset<Row> applyImpl(final QueryResolver resolver, final Dataset<Row> input, final Use<T> typeOfId) {

        final LinkableSchema linkSchema = link.getSchema();

        final Dataset<Row> joinTo = resolver.resolve(linkSchema, Constant.TRUE, ImmutableList.of(), ImmutableSet.of())
                .dataset();

        final StructType joinToType = projectKeysType(joinTo.schema(), rightKeyTypes);

        final Dataset<Row> joinToKeyed = joinTo.map(SparkUtils.map(r -> projectRightKeys(joinToType, r)), RowEncoder.apply(joinToType));

        final Function<Name, Column> columnResolver = columnResolver(input, joinToKeyed);

        final Column condition = new SparkExpressionVisitor(columnResolver)
                .visit(closedExpression);

        final Dataset<Tuple2<Row, Row>> joined = input.joinWith(joinToKeyed, condition, "left_outer");

        final DataType linkType;
        if (link.isSingle()) {
            linkType = cleanKeys(joinToType, rightKeyTypes);
        } else {
            linkType = DataTypes.createArrayType(cleanKeys(joinToType, rightKeyTypes));
        }

        final KeyValueGroupedDataset<T, Tuple2<Row, Row>> grouped = groupResults(joined);

        final StructType outputType = expandedType(root, ImmutableSet.of(name), cleanKeys(input.schema(), leftKeyTypes), linkType);

        if (next != null) {

            // Fuse the initial flat map part of the next step

            final StructType projectedType = next.projectKeysType(outputType);
            return next.apply(resolver, grouped.flatMapGroups(SparkUtils.flatMapGroups((ignored, tuples) -> {

                final Row resolved = applyLink(root, name, cleanKeys(SparkRowUtils.nulled(tuples)));
                return next.projectKeys(projectedType, resolved);

            }), RowEncoder.apply(projectedType)));

        } else {

            return grouped.mapGroups(
                    SparkUtils.mapGroups((ignored, tuples) -> applyLink(root, name, cleanKeys(SparkRowUtils.nulled(tuples)))),
                    RowEncoder.apply(outputType)
            );
        }
    }

    private Iterator<Tuple2<Row, Row>> cleanKeys(final Iterator<Tuple2<Row, Row>> tuples) {

        return Iterators.transform(tuples, tuple -> Tuple2.apply(cleanKeys(tuple._1(), leftKeyTypes), cleanKeys(tuple._2(), rightKeyTypes)));
    }

    private static Row cleanKeys(final Row row, final Map<String, Use<?>> keys) {

        if(row == null) {
            return null;
        }
        Row clean = row;
        for(final String key : keys.keySet()) {
            clean = SparkRowUtils.remove(clean, key);
        }
        return clean;
    }

    private static StructType cleanKeys(final StructType type, final Map<String, Use<?>> keys) {

        StructType clean = type;
        for(final String key : keys.keySet()) {
            clean = SparkRowUtils.remove(clean, key);
        }
        return clean;
    }

    private Function<Name, Column> columnResolver(final Dataset<Row> left, final Dataset<Row> right) {

        return name -> {
            final String first = name.first();
            if(leftConstants.containsKey(first)) {
                return SparkRowUtils.resolveName(left, name);
            } else if(rightConstants.containsKey(first)) {
                return  SparkRowUtils.resolveName(right, name);
            } else {
                throw new IllegalStateException("Link expression of the form " + link.getExpression() + " not supported");
            }
        };
    }

    private static Set<Row> linkKeys(final InstanceSchema schema, final Name name, final Row row) {

        final Set<Row> linkKeys = new HashSet<>();
        final Member member = schema.getMember(name.first(), true);
        if (member != null) {
            if (member instanceof Link && name.size() == 1) {
                linkKeys.add(row);
            } else {
                linkKeys.addAll(linkKeys(member.typeOf(), name.withoutFirst(), SparkRowUtils.get(row, name.first())));
            }
        }
        return linkKeys;
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
                        return applyLink(member.typeOf(), name.withoutFirst(), oldValue, value);
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
