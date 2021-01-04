package io.basestar.storage.sql;

/*-
 * #%L
 * basestar-storage-sql
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
import io.basestar.expression.bitwise.*;
import io.basestar.expression.compare.*;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.constant.NameConstant;
import io.basestar.expression.function.Coalesce;
import io.basestar.expression.function.IfElse;
import io.basestar.expression.function.In;
import io.basestar.expression.iterate.ContextIterator;
import io.basestar.expression.iterate.ForAny;
import io.basestar.expression.logical.And;
import io.basestar.expression.logical.Not;
import io.basestar.expression.logical.Or;
import io.basestar.expression.text.Like;
import io.basestar.expression.type.Coercion;
import io.basestar.util.Name;
import lombok.RequiredArgsConstructor;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.QueryPart;
import org.jooq.impl.DSL;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Function;

@RequiredArgsConstructor
public class SQLExpressionVisitor implements ExpressionVisitor.Defaulting<QueryPart> {

    private final Function<Name, QueryPart> columnResolver;

    @Override
    public QueryPart visitDefault(final Expression expression) {

        return null;
    }

    @Override
    public QueryPart visitAdd(final Add expression) {

        final Field<Object> lhs = SQLUtils.field(expression.getLhs().visit(this), Object.class);
        final Field<Number> rhs = SQLUtils.field(expression.getRhs().visit(this), Number.class);
        return (lhs != null && rhs != null) ? lhs.add(rhs) : null;
    }

    @Override
    public QueryPart visitDiv(final Div expression) {

        final Field<Object> lhs = SQLUtils.field(expression.getLhs().visit(this), Object.class);
        final Field<Number> rhs = SQLUtils.field(expression.getRhs().visit(this), Number.class);
        return (lhs != null && rhs != null) ? lhs.div(rhs) : null;
    }

    @Override
    public QueryPart visitMod(final Mod expression) {

        final Field<Object> lhs = SQLUtils.field(expression.getLhs().visit(this), Object.class);
        final Field<Number> rhs = SQLUtils.field(expression.getRhs().visit(this), Number.class);
        return (lhs != null && rhs != null) ? lhs.mod(rhs) : null;
    }

    @Override
    public QueryPart visitMul(final Mul expression) {

        final Field<Object> lhs = SQLUtils.field(expression.getLhs().visit(this), Object.class);
        final Field<Number> rhs = SQLUtils.field(expression.getRhs().visit(this), Number.class);
        return (lhs != null && rhs != null) ? lhs.mul(rhs) : null;
    }

    @Override
    public QueryPart visitNegate(final Negate expression) {

        final Field<Object> with = SQLUtils.field(expression.getOperand().visit(this), Object.class);
        return with != null ? with.neg() : null;
    }

    @Override
    public QueryPart visitPow(final Pow expression) {

        final Field<Object> lhs = SQLUtils.field(expression.getLhs().visit(this), Object.class);
        final Field<Number> rhs = SQLUtils.field(expression.getRhs().visit(this), Number.class);
        return (lhs != null && rhs != null) ? lhs.pow(rhs) : null;
    }

    @Override
    public QueryPart visitSub(final Sub expression) {

        final Field<Object> lhs = SQLUtils.field(expression.getLhs().visit(this), Object.class);
        final Field<Number> rhs = SQLUtils.field(expression.getRhs().visit(this), Number.class);
        return (lhs != null && rhs != null) ? lhs.sub(rhs) : null;
    }

    @Override
    public QueryPart visitBitAnd(final BitAnd expression) {

        final Field<Object> lhs = SQLUtils.field(expression.getLhs().visit(this), Object.class);
        final Field<Object> rhs = SQLUtils.field(expression.getRhs().visit(this), Object.class);
        return (lhs != null && rhs != null) ? lhs.bitAnd(rhs) : null;
    }

    @Override
    public QueryPart visitBitFlip(final BitNot expression) {

        final Field<Object> with = SQLUtils.field(expression.getOperand().visit(this), Object.class);
        return with != null ? with.bitNot() : null;
    }

    @Override
    public QueryPart visitBitLsh(final BitLsh expression) {

        final Field<Object> lhs = SQLUtils.field(expression.getLhs().visit(this), Object.class);
        final Field<Number> rhs = SQLUtils.field(expression.getRhs().visit(this), Number.class);
        return (lhs != null && rhs != null) ? lhs.shl(rhs) : null;
    }

    @Override
    public QueryPart visitBitOr(final BitOr expression) {

        final Field<Object> lhs = SQLUtils.field(expression.getLhs().visit(this), Object.class);
        final Field<Object> rhs = SQLUtils.field(expression.getRhs().visit(this), Object.class);
        return (lhs != null && rhs != null) ? lhs.bitOr(rhs) : null;
    }

    @Override
    public QueryPart visitBitRsh(final BitRsh expression) {

        final Field<Object> lhs = SQLUtils.field(expression.getLhs().visit(this), Object.class);
        final Field<Number> rhs = SQLUtils.field(expression.getRhs().visit(this), Number.class);
        return (lhs != null && rhs != null) ? lhs.shr(rhs) : null;
    }

    @Override
    public QueryPart visitBitXor(final BitXor expression) {

        final Field<Object> lhs = SQLUtils.field(expression.getLhs().visit(this), Object.class);
        final Field<Object> rhs = SQLUtils.field(expression.getRhs().visit(this), Object.class);
        return (lhs != null && rhs != null) ? lhs.bitXor(rhs) : null;
    }

    @Override
    public QueryPart visitCmp(final Cmp expression) {

        throw new UnsupportedOperationException();
    }

    @Override
    public QueryPart visitEq(final Eq expression) {

        final Field<Object> lhs = SQLUtils.field(expression.getLhs().visit(this), Object.class);
        final Field<Object> rhs = SQLUtils.field(expression.getRhs().visit(this), Object.class);
        return (lhs != null && rhs != null) ? lhs.eq(rhs) : null;
    }

    @Override
    public QueryPart visitGt(final Gt expression) {

        final Field<Object> lhs = SQLUtils.field(expression.getLhs().visit(this), Object.class);
        final Field<Object> rhs = SQLUtils.field(expression.getRhs().visit(this), Object.class);
        return (lhs != null && rhs != null) ? lhs.gt(rhs) : null;
    }

    @Override
    public QueryPart visitGte(final Gte expression) {

        final Field<Object> lhs = SQLUtils.field(expression.getLhs().visit(this), Object.class);
        final Field<Object> rhs = SQLUtils.field(expression.getRhs().visit(this), Object.class);
        return (lhs != null && rhs != null) ? lhs.ge(rhs) : null;
    }

    @Override
    public QueryPart visitLt(final Lt expression) {

        final Field<Object> lhs = SQLUtils.field(expression.getLhs().visit(this), Object.class);
        final Field<Object> rhs = SQLUtils.field(expression.getRhs().visit(this), Object.class);
        return (lhs != null && rhs != null) ? lhs.lt(rhs) : null;
    }

    @Override
    public QueryPart visitLte(final Lte expression) {

        final Field<Object> lhs = SQLUtils.field(expression.getLhs().visit(this), Object.class);
        final Field<Object> rhs = SQLUtils.field(expression.getRhs().visit(this), Object.class);
        return (lhs != null && rhs != null) ? lhs.le(rhs) : null;
    }

    @Override
    public QueryPart visitNe(final Ne expression) {

        final Field<Object> lhs = SQLUtils.field(expression.getLhs().visit(this), Object.class);
        final Field<Object> rhs = SQLUtils.field(expression.getRhs().visit(this), Object.class);
        return (lhs != null && rhs != null) ? lhs.ne(rhs) : null;
    }

    @Override
    public QueryPart visitConstant(final Constant expression) {

        // Use inline rather than val because some JDBC drivers (Athena) don't support positional params
        return DSL.inline(expression.getValue());
    }

    @Override
    public QueryPart visitNameConstant(final NameConstant expression) {

        final Name name = expression.getName();
        return columnResolver.apply(name);
    }

    @Override
    public QueryPart visitCoalesce(final Coalesce expression) {

        final Field<Object> lhs = SQLUtils.field(expression.getLhs().visit(this), Object.class);
        final Field<Object> rhs = SQLUtils.field(expression.getRhs().visit(this), Object.class);
        return (lhs != null && rhs != null) ? DSL.coalesce(lhs, rhs) : null;
    }

    @Override
    public QueryPart visitLike(final Like expression) {

        final Field<Object> lhs = SQLUtils.field(expression.getLhs().visit(this), Object.class);
        final Field<String> rhs = SQLUtils.field(expression.getRhs().visit(this), String.class);
        return (lhs != null && rhs != null) ? (expression.isCaseSensitive() ? lhs.like(rhs) : lhs.likeIgnoreCase(rhs)) : null;
    }

    @Override
    public QueryPart visitIfElse(final IfElse expression) {

        throw new UnsupportedOperationException();
    }

    @Override
    public QueryPart visitIn(final In expression) {

        final Field<Object> lhs = SQLUtils.field(expression.getLhs().visit(this), Object.class);
        final Field<Object> rhs = SQLUtils.field(expression.getRhs().visit(this), Object.class);
        return (lhs != null && rhs != null) ? lhs.in(rhs) : null;
    }

    @Override
    public QueryPart visitForAny(final ForAny expression) {

        final Expression lhs = expression.getYield();
        final ContextIterator iterator = expression.getIterator();
        // map keys not supported
        if(iterator instanceof ContextIterator.OfValue) {
            final ContextIterator.OfValue of = (ContextIterator.OfValue)iterator;
            if(of.getExpr() instanceof NameConstant) {
                final Name rhsName = ((NameConstant) of.getExpr()).getName();
                final String first = of.getValue();
                final Expression bound = lhs.bind(Context.init(), path -> {
                    if(first.equals(path.first())) {
                        return rhsName.with(path.withoutFirst());
                    } else {
                        return path;
                    }
                });
                return visit(bound);
            }
        }
        return null;
    }

    @Override
    public QueryPart visitAnd(final And expression) {

        return DSL.and(expression.getTerms().stream()
                .map(v -> SQLUtils.condition(v.visit(this)))
                .filter(Objects::nonNull)
                .toArray(Condition[]::new));
    }

    @Override
    public QueryPart visitNot(final Not expression) {

        final Condition cond = SQLUtils.condition(expression.visit(this));
        return cond != null ? DSL.not(cond) : null;
    }

    @Override
    public QueryPart visitOr(final Or expression) {

        final Condition[] conditions = expression.getTerms().stream().map(v -> SQLUtils.condition(v.visit(this)))
                .toArray(Condition[]::new);
        // Unlike and, if any OR elements are null we can't create a condition
        if(Arrays.stream(conditions).allMatch(Objects::nonNull)) {
            return DSL.or(conditions);
        } else {
            return null;
        }
    }

    @Override
    public Field<?> visitSum(final Sum aggregate) {

        final Field<? extends Number> input = SQLUtils.cast(field(aggregate.getInput()), Number.class);
        return DSL.sum(input);
    }

    @Override
    public Field<?> visitMin(final Min aggregate) {

        final Field<?> input = field(aggregate.getInput());
        return DSL.min(input);
    }

    @Override
    public Field<?> visitMax(final Max aggregate) {

        final Field<?> input = field(aggregate.getInput());
        return DSL.max(input);
    }

    @Override
    public Field<?> visitAvg(final Avg aggregate) {

        final Field<? extends Number> input = SQLUtils.cast(field(aggregate.getInput()), Number.class);
        return DSL.avg(input);
    }

    @Override
    public Field<?> visitCount(final Count aggregate) {

        if(aggregate.getPredicate() instanceof Constant) {
            final boolean value = Coercion.isTruthy(((Constant) aggregate.getPredicate()).getValue());
            if(value) {
                return DSL.count();
            } else {
                return DSL.inline(0);
            }
        } else {
            final Field<?> input = field(aggregate.getPredicate());
            return DSL.count(DSL.nullif(input, false));
        }
    }

    public Condition condition(final Expression expression) {

        return SQLUtils.condition(visit(expression));
    }

    public Field<?> field(final Expression expression) {

        return SQLUtils.field(visit(expression));
    }
}
