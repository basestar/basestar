package io.basestar.spark.expression;

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

import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.aggregate.*;
import io.basestar.expression.arithmetic.*;
import io.basestar.expression.call.Callable;
import io.basestar.expression.call.MemberCall;
import io.basestar.expression.compare.*;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.constant.NameConstant;
import io.basestar.expression.function.Coalesce;
import io.basestar.expression.function.IfElse;
import io.basestar.expression.function.Index;
import io.basestar.expression.literal.LiteralObject;
import io.basestar.expression.logical.And;
import io.basestar.expression.logical.Not;
import io.basestar.expression.logical.Or;
import io.basestar.expression.methods.Methods;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.expression.InferenceVisitor;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseNumeric;
import io.basestar.schema.use.UseStringLike;
import io.basestar.spark.util.SparkSchemaUtils;
import io.basestar.util.Name;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.lit;

@RequiredArgsConstructor
public class SparkExpressionVisitor implements ExpressionVisitor.Defaulting<Column> {

    private final Function<Name, Column> columnResolver;

    private final InferenceContext inferenceContext;

    private final Methods methods;

    private final Context context;

    public SparkExpressionVisitor(final Function<Name, Column> columnResolver, final InferenceContext inferenceContext) {

        this.columnResolver = columnResolver;
        this.inferenceContext = inferenceContext;
        this.methods = Methods.builder().defaults().build();
        this.context = Context.init(methods);
    }

    @Override
    public Column visitDefault(final Expression expression) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Column visitIndex(final Index expression) {

        final Column lhs = visit(expression.getLhs());
        final Column rhs = visit(expression.getRhs());
        final Use<?> rhsType = typeOf(expression.getRhs());
        if(rhsType instanceof UseNumeric) {
            return functions.element_at(lhs, rhs.plus(1));
        } else {
            return functions.element_at(lhs, rhs);
        }
    }

    @Override
    public Column visitLiteralObject(final LiteralObject expression) {

        final Column[] columns = expression.getArgs().entrySet().stream()
                .map(e -> visit(e.getValue()).as(e.getKey().evaluateAs(String.class, context)))
                .toArray(Column[]::new);
        return functions.struct(columns);
    }

    @Override
    public Column visitAdd(final Add expression) {

        final Column lhs = visit(expression.getLhs());
        final Column rhs = visit(expression.getRhs());
        final Use<?> type = typeOf(expression);
        if(type instanceof UseStringLike<?>) {
            return functions.concat(lhs.cast(DataTypes.StringType), rhs.cast(DataTypes.StringType));
        } else {
            return lhs.plus(rhs);
        }
    }

    @Override
    public Column visitDiv(final Div expression) {

        final Column lhs = visit(expression.getLhs());
        final Column rhs = visit(expression.getRhs());
        return lhs.divide(rhs);
    }

    @Override
    public Column visitMul(final Mul expression) {

        final Column lhs = visit(expression.getLhs());
        final Column rhs = visit(expression.getRhs());
        return lhs.multiply(rhs);
    }

    @Override
    public Column visitNegate(final Negate expression) {

        final Column op = visit(expression.getOperand());
        return op.unary_$minus();
    }

    @Override
    public Column visitSub(final Sub expression) {

        final Column lhs = visit(expression.getLhs());
        final Column rhs = visit(expression.getRhs());
        return lhs.minus(rhs);
    }

    @Override
    public Column visitPow(final Pow expression) {

        final Column lhs = visit(expression.getLhs());
        final Column rhs = visit(expression.getRhs());
        return functions.pow(lhs, rhs);
    }

    @Override
    public Column visitEq(final Eq expression) {

        final Column lhs = visit(expression.getLhs());
        final Column rhs = visit(expression.getRhs());
        return lhs.eqNullSafe(rhs);
    }

    @Override
    public Column visitGt(final Gt expression) {

        final Column lhs = visit(expression.getLhs());
        final Column rhs = visit(expression.getRhs());
        return lhs.gt(rhs);
    }

    @Override
    public Column visitGte(final Gte expression) {

        final Column lhs = visit(expression.getLhs());
        final Column rhs = visit(expression.getRhs());
        return lhs.geq(rhs);
    }

    @Override
    public Column visitLt(final Lt expression) {

        final Column lhs = visit(expression.getLhs());
        final Column rhs = visit(expression.getRhs());
        return lhs.lt(rhs);
    }

    @Override
    public Column visitLte(final Lte expression) {

        final Column lhs = visit(expression.getLhs());
        final Column rhs = visit(expression.getRhs());
        return lhs.leq(rhs);
    }

    @Override
    public Column visitNe(final Ne expression) {

        final Column lhs = visit(expression.getLhs());
        final Column rhs = visit(expression.getRhs());
        return lhs.eqNullSafe(rhs).unary_$bang();
    }

    @Override
    public Column visitConstant(final Constant expression) {

        final Object value = expression.getValue();
        return literal(value);
    }

    @SuppressWarnings("unchecked")
    private static Column literal(final Object value) {

        if(value instanceof Collection) {
            final Column[] columns = ((Collection<?>)value).stream()
                    .map(SparkExpressionVisitor::literal)
                    .toArray(Column[]::new);
            return functions.array(columns);
        } else if(value instanceof Map) {
            final Column[] columns = ((Map<String, ?>)value).entrySet().stream()
                    .flatMap(e -> Stream.of(functions.lit(e.getKey()), literal(e.getValue())))
                    .toArray(Column[]::new);
            return functions.map(columns);
        } else {
            return functions.lit(value);
        }
    }

    @Override
    public Column visitNameConstant(final NameConstant expression) {

        return columnResolver.apply(expression.getName());
    }

    @Override
    public Column visitMemberCall(final MemberCall expression) {

        final Column with = this.visit(expression.getWith());
        final Column[] args = expression.getArgs().stream().map(this::visit).toArray(Column[]::new);
        final Type withType = SparkSchemaUtils.type(with.expr().dataType()).javaType();
        final Type[] argTypes = expression.getArgs().stream().map(v -> typeOf(v).javaType()).toArray(Type[]::new);
        final Callable callable = context.callable(withType, expression.getMember(), argTypes);

        final UserDefinedFunction udf = SparkSchemaUtils.udf(callable);
        final Column[] mergedArgs = Stream.concat(Stream.of(with), Arrays.stream(args)).toArray(Column[]::new);
        return udf.apply(mergedArgs);
    }

    private Use<?> typeOf(final Expression expression) {

        return new InferenceVisitor(inferenceContext).visit(expression);
    }

    @Override
    public Column visitCoalesce(final Coalesce expression) {

        final Column lhs = visit(expression.getLhs());
        final Column rhs = visit(expression.getRhs());
        return coalesce(lhs, rhs);
    }

    @Override
    public Column visitAnd(final And expression) {

        Column current = null;
        for(final Expression term : expression.getTerms()) {
            if(current == null) {
                current = visit(term);
            } else {
                current = current.and(visit(term));
            }
        }
        return current == null ? lit(true) : current;
    }

    @Override
    public Column visitNot(final Not expression) {

        return visit(expression.getOperand()).unary_$bang();
    }

    @Override
    public Column visitOr(final Or expression) {

        Column current = null;
        for(final Expression term : expression.getTerms()) {
            if(current == null) {
                current = visit(term);
            } else {
                current = current.or(visit(term));
            }
        }
        return current == null ? lit(false) : current;
    }

    @Override
    public Column visitIfElse(final IfElse expression) {

        return functions.when(visit(expression.getPredicate()), visit(expression.getThen())).otherwise(visit(expression.getOtherwise()));
    }

    @Override
    public Column visitSum(final Sum expression) {

        final Column input = visit(expression.getInput());
        return functions.sum(input);
    }

    @Override
    public Column visitCollectArray(final CollectArray expression) {

        final Column input = visit(expression.getInput());
        return functions.collect_list(input);
    }

    @Override
    public Column visitMin(final Min expression) {

        final Column input = visit(expression.getInput());
        return functions.min(input);
    }

    @Override
    public Column visitMax(final Max expression) {

        final Column input = visit(expression.getInput());
        return functions.max(input);
    }

    @Override
    public Column visitAvg(final Avg expression) {

        final Column input = visit(expression.getInput());
        return functions.avg(input);
    }

    @Override
    public Column visitCount(final Count expression) {

        final Column input = visit(expression.getPredicate());
        return functions.count(input);
    }
}
