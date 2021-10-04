package io.basestar.spark.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.Layout;
import io.basestar.schema.ReferableSchema;
import io.basestar.schema.Reserved;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.expression.InferenceVisitor;
import io.basestar.schema.use.Use;
import io.basestar.spark.expression.ClosureExtractingVisitor;
import io.basestar.spark.expression.SparkExpressionVisitor;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;
import java.util.function.Function;

public class CascadeJoiner {

    private static final String L_PREFIX = Reserved.PREFIX + "l_";

    private static final String R_PREFIX = Reserved.PREFIX + "r_";

    private final ReferableSchema fromSchema;

    private final ReferableSchema toSchema;

    private final StructType resultType;

    private final Set<Expression> expressions;

    public CascadeJoiner(final ReferableSchema fromSchema, final ReferableSchema toSchema, final Set<Expression> expressions) {

        this.fromSchema = fromSchema;
        this.toSchema = toSchema;
        this.expressions = expressions;
        this.resultType = SparkSchemaUtils.structType(toSchema);
        if (expressions.isEmpty()) {
            throw new IllegalStateException("Must supply at least one cascade expression");
        }
    }

    public Dataset<Row> cascade(final Dataset<Row> from, final Dataset<Row> to) {

        return expressions.stream().map(expression -> cascade(fromSchema, toSchema, resultType, from, to, expression))
                .reduce(Dataset::union)
                .map(ds -> ds.dropDuplicates(new String[]{ReferableSchema.ID}))
                .orElseThrow(IllegalStateException::new);
    }

    private static Dataset<Row> cascade(final ReferableSchema fromSchema, final ReferableSchema toSchema, final StructType resultType, final Dataset<Row> from, final Dataset<Row> to, final Expression expression) {

        final ClosureExtractingVisitor closeLeft = new ClosureExtractingVisitor(L_PREFIX, Reserved.THIS::equals);
        final Expression closedLeft = closeLeft.visit(expression);
        final SortedMap<String, Expression> leftConstants = Immutable.sortedMap(closeLeft.getConstants());
        if (leftConstants.isEmpty()) {
            throw new IllegalStateException("Link expression must have constants on left side");
        }

        final ClosureExtractingVisitor closeRight = new ClosureExtractingVisitor(R_PREFIX, n -> !Reserved.THIS.equals(n) && !n.startsWith(L_PREFIX));
        final Expression closedExpression = closeRight.visit(closedLeft);
        final SortedMap<String, Expression> rightConstants = Immutable.sortedMap(closeRight.getConstants());

        final InferenceContext inferenceContext = InferenceContext.from(toSchema)
                .overlay(Reserved.THIS, InferenceContext.from(fromSchema));
        final InferenceVisitor inference = new InferenceVisitor(inferenceContext);

        final SortedMap<String, Use<?>> leftLayout = new TreeMap<>();
        for (final Map.Entry<String, Expression> entry : leftConstants.entrySet()) {
            leftLayout.put(entry.getKey(), inference.typeOf(entry.getValue()));
        }
        final StructType leftType = SparkSchemaUtils.structType(leftLayout, null);

        final SortedMap<String, Use<?>> rightLayout = new TreeMap<>();
        rightLayout.putAll(toSchema.layoutSchema(ImmutableSet.of()));
        for (final Map.Entry<String, Expression> entry : rightConstants.entrySet()) {
            rightLayout.put(entry.getKey(), inference.typeOf(entry.getValue()));
        }
        final StructType rightType = SparkSchemaUtils.structType(rightLayout, null);

        final Dataset<Row> left = from.map(SparkUtils.map(row -> {
            final Map<String, Object> instance = SparkSchemaUtils.fromSpark(fromSchema, row);
            final Map<String, Object> result = new HashMap<>();
            for (final Map.Entry<String, Expression> entry : leftConstants.entrySet()) {
                result.put(entry.getKey(), entry.getValue().evaluate(Context.init(ImmutableMap.of(Reserved.THIS, instance))));
            }
            return SparkSchemaUtils.toSpark(Layout.simple(leftLayout), leftType, result);
        }), RowEncoder.apply(leftType)).dropDuplicates();

        final Dataset<Row> right = to.map(SparkUtils.map(row -> {
            final Map<String, Object> instance = SparkSchemaUtils.fromSpark(toSchema, row);
            final Map<String, Object> result = new HashMap<>(instance);
            for (final Map.Entry<String, Expression> entry : rightConstants.entrySet()) {
                result.put(entry.getKey(), entry.getValue().evaluate(Context.init(instance)));
            }
            return SparkSchemaUtils.toSpark(Layout.simple(rightLayout), rightType, result);
        }), RowEncoder.apply(rightType));

        final Function<Name, Column> columnResolver = name -> {
            final String first = name.first();
            if (leftConstants.containsKey(first)) {
                return SparkRowUtils.resolveName(left, name);
            } else if (rightConstants.containsKey(first)) {
                return SparkRowUtils.resolveName(right, name);
            } else {
                throw new IllegalStateException("Cascade expression of the form " + expression + " not supported");
            }
        };

        final Column condition = new SparkExpressionVisitor(columnResolver)
                .visit(closedExpression);

        final Dataset<Tuple2<Row, Row>> joined = left.joinWith(right, condition, "inner");

        return joined.map(SparkUtils.map(v -> SparkRowUtils.conform(v._2(), resultType)), RowEncoder.apply(resultType));
    }
}
