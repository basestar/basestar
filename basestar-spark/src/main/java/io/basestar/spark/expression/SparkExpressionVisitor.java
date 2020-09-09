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
import io.basestar.expression.arithmetic.*;
import io.basestar.expression.compare.*;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.constant.NameConstant;
import io.basestar.expression.function.Coalesce;
import io.basestar.expression.function.Index;
import io.basestar.expression.literal.LiteralObject;
import io.basestar.expression.logical.And;
import io.basestar.expression.logical.Not;
import io.basestar.expression.logical.Or;
import io.basestar.util.Name;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StringType;

import java.util.function.Function;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.lit;

@RequiredArgsConstructor
public class SparkExpressionVisitor implements ExpressionVisitor.Defaulting<Column> {

    private final Function<Name, Column> columnResolver;

    @Override
    public Column visitDefault(final Expression expression) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Column visitIndex(final Index expression) {

        // FIXME: context should be a constructor arg
        final Context context = Context.init();
        // FIXME better logging when the type is wrong
        final int at = expression.getRhs().evaluateAs(Number.class, context).intValue();
        return functions.element_at(visit(expression.getLhs()), at + 1);
    }

    @Override
    public Column visitLiteralObject(final LiteralObject expression) {

        // FIXME: context should be a constructor arg
        final Context context = Context.init();
        final Column[] columns = expression.getArgs().entrySet().stream()
                .map(e -> visit(e.getValue()).as(e.getKey().evaluateAs(String.class, context)))
                .toArray(Column[]::new);
        return functions.struct(columns);
    }

    @Override
    public Column visitAdd(final Add expression) {

        final Column lhs = visit(expression.getLhs());
        final Column rhs = visit(expression.getRhs());
        // FIXME add concat for lists and maps here
        if(lhs.expr().dataType() instanceof StringType || rhs.expr().dataType() instanceof StringType) {
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

        return lit(expression.getValue());
    }

    @Override
    public Column visitNameConstant(final NameConstant expression) {

        return columnResolver.apply(expression.getName());
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

        return visit(expression).unary_$bang();
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
}
