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
import io.basestar.expression.compare.*;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.constant.NameConstant;
import io.basestar.expression.function.Coalesce;
import io.basestar.expression.function.IfElse;
import io.basestar.expression.literal.LiteralObject;
import io.basestar.expression.logical.And;
import io.basestar.expression.logical.Not;
import io.basestar.expression.logical.Or;
import io.basestar.util.Name;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public class SparkExpressionVisitor implements ExpressionVisitor.Defaulting<Column> {

    private final Function<Name, Column> columnResolver;

    public SparkExpressionVisitor(final Function<Name, Column> columnResolver) {

        this.columnResolver = columnResolver;
    }

    @Override
    public Column visitDefault(final Expression expression) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Column visitLiteralObject(final LiteralObject expression) {

        final Context context = Context.init();
        final Column[] columns = expression.getArgs().entrySet().stream()
                .map(e -> visit(e.getValue()).as(e.getKey().evaluateAs(String.class, context)))
                .toArray(Column[]::new);
        return functions.struct(columns);
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
    public Column visitCoalesce(final Coalesce expression) {

        final Column lhs = visit(expression.getLhs());
        final Column rhs = visit(expression.getRhs());
        return functions.coalesce(lhs, rhs);
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
        return current == null ? functions.lit(true) : current;
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
        return current == null ? functions.lit(false) : current;
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
