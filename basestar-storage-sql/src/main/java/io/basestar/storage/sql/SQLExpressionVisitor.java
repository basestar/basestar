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
import io.basestar.expression.arithmetic.*;
import io.basestar.expression.bitwise.*;
import io.basestar.expression.compare.*;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.constant.PathConstant;
import io.basestar.expression.function.*;
import io.basestar.expression.iterate.*;
import io.basestar.expression.literal.LiteralArray;
import io.basestar.expression.literal.LiteralObject;
import io.basestar.expression.literal.LiteralSet;
import io.basestar.expression.logical.And;
import io.basestar.expression.logical.Not;
import io.basestar.expression.logical.Or;
import io.basestar.util.Path;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Name;
import org.jooq.QueryPart;
import org.jooq.impl.DSL;

import java.util.Arrays;
import java.util.Objects;

public class SQLExpressionVisitor implements ExpressionVisitor<QueryPart> {

    @Override
    public QueryPart visitAdd(final Add expression) {

        final Field<Object> lhs = field(expression.getLhs().visit(this), Object.class);
        final Field<Number> rhs = field(expression.getRhs().visit(this), Number.class);
        return (lhs != null && rhs != null) ? lhs.add(rhs) : null;
    }

    @Override
    public QueryPart visitDiv(final Div expression) {

        final Field<Object> lhs = field(expression.getLhs().visit(this), Object.class);
        final Field<Number> rhs = field(expression.getRhs().visit(this), Number.class);
        return (lhs != null && rhs != null) ? lhs.div(rhs) : null;
    }

    @Override
    public QueryPart visitMod(final Mod expression) {

        final Field<Object> lhs = field(expression.getLhs().visit(this), Object.class);
        final Field<Number> rhs = field(expression.getRhs().visit(this), Number.class);
        return (lhs != null && rhs != null) ? lhs.mod(rhs) : null;
    }

    @Override
    public QueryPart visitMul(final Mul expression) {

        final Field<Object> lhs = field(expression.getLhs().visit(this), Object.class);
        final Field<Number> rhs = field(expression.getRhs().visit(this), Number.class);
        return (lhs != null && rhs != null) ? lhs.mul(rhs) : null;
    }

    @Override
    public QueryPart visitNegate(final Negate expression) {

        final Field<Object> with = field(expression.getOperand().visit(this), Object.class);
        return with != null ? with.neg() : null;
    }

    @Override
    public QueryPart visitPow(final Pow expression) {

        final Field<Object> lhs = field(expression.getLhs().visit(this), Object.class);
        final Field<Number> rhs = field(expression.getRhs().visit(this), Number.class);
        return (lhs != null && rhs != null) ? lhs.pow(rhs) : null;
    }

    @Override
    public QueryPart visitSub(final Sub expression) {

        final Field<Object> lhs = field(expression.getLhs().visit(this), Object.class);
        final Field<Number> rhs = field(expression.getRhs().visit(this), Number.class);
        return (lhs != null && rhs != null) ? lhs.sub(rhs) : null;
    }

    @Override
    public QueryPart visitBitAnd(final BitAnd expression) {

        final Field<Object> lhs = field(expression.getLhs().visit(this), Object.class);
        final Field<Object> rhs = field(expression.getRhs().visit(this), Object.class);
        return (lhs != null && rhs != null) ? lhs.bitAnd(rhs) : null;
    }

    @Override
    public QueryPart visitBitFlip(final BitNot expression) {

        final Field<Object> with = field(expression.getOperand().visit(this), Object.class);
        return with != null ? with.bitNot() : null;
    }

    @Override
    public QueryPart visitBitLsh(final BitLsh expression) {

        final Field<Object> lhs = field(expression.getLhs().visit(this), Object.class);
        final Field<Number> rhs = field(expression.getRhs().visit(this), Number.class);
        return (lhs != null && rhs != null) ? lhs.shl(rhs) : null;
    }

    @Override
    public QueryPart visitBitOr(final BitOr expression) {

        final Field<Object> lhs = field(expression.getLhs().visit(this), Object.class);
        final Field<Object> rhs = field(expression.getRhs().visit(this), Object.class);
        return (lhs != null && rhs != null) ? lhs.bitOr(rhs) : null;
    }

    @Override
    public QueryPart visitBitRsh(final BitRsh expression) {

        final Field<Object> lhs = field(expression.getLhs().visit(this), Object.class);
        final Field<Number> rhs = field(expression.getRhs().visit(this), Number.class);
        return (lhs != null && rhs != null) ? lhs.shr(rhs) : null;
    }

    @Override
    public QueryPart visitBitXor(final BitXor expression) {

        final Field<Object> lhs = field(expression.getLhs().visit(this), Object.class);
        final Field<Object> rhs = field(expression.getRhs().visit(this), Object.class);
        return (lhs != null && rhs != null) ? lhs.bitXor(rhs) : null;
    }

    @Override
    public QueryPart visitCmp(final Cmp expression) {

        throw new UnsupportedOperationException();
    }

    @Override
    public QueryPart visitEq(final Eq expression) {

        final Field<Object> lhs = field(expression.getLhs().visit(this), Object.class);
        final Field<Object> rhs = field(expression.getRhs().visit(this), Object.class);
        return (lhs != null && rhs != null) ? lhs.eq(rhs) : null;
    }

    @Override
    public QueryPart visitGt(final Gt expression) {

        final Field<Object> lhs = field(expression.getLhs().visit(this), Object.class);
        final Field<Object> rhs = field(expression.getRhs().visit(this), Object.class);
        return (lhs != null && rhs != null) ? lhs.gt(rhs) : null;
    }

    @Override
    public QueryPart visitGte(final Gte expression) {

        final Field<Object> lhs = field(expression.getLhs().visit(this), Object.class);
        final Field<Object> rhs = field(expression.getRhs().visit(this), Object.class);
        return (lhs != null && rhs != null) ? lhs.ge(rhs) : null;
    }

    @Override
    public QueryPart visitLt(final Lt expression) {

        final Field<Object> lhs = field(expression.getLhs().visit(this), Object.class);
        final Field<Object> rhs = field(expression.getRhs().visit(this), Object.class);
        return (lhs != null && rhs != null) ? lhs.lt(rhs) : null;
    }

    @Override
    public QueryPart visitLte(final Lte expression) {

        final Field<Object> lhs = field(expression.getLhs().visit(this), Object.class);
        final Field<Object> rhs = field(expression.getRhs().visit(this), Object.class);
        return (lhs != null && rhs != null) ? lhs.le(rhs) : null;
    }

    @Override
    public QueryPart visitNe(final Ne expression) {

        final Field<Object> lhs = field(expression.getLhs().visit(this), Object.class);
        final Field<Object> rhs = field(expression.getRhs().visit(this), Object.class);
        return (lhs != null && rhs != null) ? lhs.ne(rhs) : null;
    }

    @Override
    public QueryPart visitConstant(final Constant expression) {

        return DSL.val(expression.getValue());
    }

    @Override
    public QueryPart visitPathConstant(final PathConstant expression) {

        final Path path = expression.getPath();
        return DSL.field(DSL.name(path.stream()
                .map(DSL::name).toArray(Name[]::new)));
    }

    @Override
    public QueryPart visitCoalesce(final Coalesce expression) {

        final Field<Object> lhs = field(expression.getLhs().visit(this), Object.class);
        final Field<Object> rhs = field(expression.getRhs().visit(this), Object.class);
        return (lhs != null && rhs != null) ? DSL.coalesce(lhs, rhs) : null;
    }

    @Override
    public QueryPart visitIfElse(final IfElse expression) {

        throw new UnsupportedOperationException();
    }

    @Override
    public QueryPart visitIn(final In expression) {

        final Field<Object> lhs = field(expression.getLhs().visit(this), Object.class);
        final Field<Object> rhs = field(expression.getRhs().visit(this), Object.class);
        return (lhs != null && rhs != null) ? lhs.in(rhs) : null;
    }

    @Override
    public QueryPart visitIndex(final Index expression) {

        return null;
    }

    @Override
    public QueryPart visitLambda(final Lambda expression) {

        return null;
    }

    @Override
    public QueryPart visitLambdaCall(final Call expression) {

        return null;
    }

    @Override
    public QueryPart visitMember(final Member expression) {

        return null;
    }

    @Override
    public QueryPart visitCall(final Call expression) {

        return null;
    }

    @Override
    public QueryPart visitWith(final With expression) {

        return null;
    }

    @Override
    public QueryPart visitForAll(final ForAll expression) {

        return null;
    }

    @Override
    public QueryPart visitForAny(final ForAny expression) {

        final Expression lhs = expression.getLhs();
        final Expression rhs = expression.getRhs();
        if(rhs instanceof Of) {
            final Of of = (Of)rhs;
            if(of.getExpr() instanceof PathConstant) {
                final Path rhsPath = ((PathConstant) of.getExpr()).getPath();
                // map keys not supported
                if(of.getKey() == null) {
                    final String first = of.getValue();
                    final Expression bound = lhs.bind(Context.init(), path -> {
                        if(first.equals(path.first())) {
                            return SQLUtils.columnPath(rhsPath.with(path.withoutFirst()));
                        } else {
                            return path;
                        }
                    });
                    return visit(bound);
                }
            }
        }
        return null;
    }

    @Override
    public QueryPart visitForArray(final ForArray expression) {

        return null;
    }

    @Override
    public QueryPart visitForObject(final ForObject expression) {

        return null;
    }

    @Override
    public QueryPart visitForSet(final ForSet expression) {

        return null;
    }

    @Override
    public QueryPart visitOf(final Of expression) {

        return null;
    }

    @Override
    public QueryPart visitWhere(final Where expression) {

        return null;
    }

    @Override
    public QueryPart visitLiteralArray(final LiteralArray expression) {

        return null;
    }

    @Override
    public QueryPart visitLiteralObject(final LiteralObject expression) {

        return null;
    }

    @Override
    public QueryPart visitLiteralSet(final LiteralSet expression) {

        return null;
    }

    @Override
    public QueryPart visitAnd(final And expression) {

        return DSL.and(expression.getTerms().stream()
                .map(v -> condition(v.visit(this)))
                .filter(Objects::nonNull)
                .toArray(Condition[]::new));
    }

    @Override
    public QueryPart visitNot(final Not expression) {

        final Condition cond = condition(expression.visit(this));
        return cond != null ? DSL.not(cond) : null;
    }

    @Override
    public QueryPart visitOr(final Or expression) {

        final Condition[] conditions = expression.getTerms().stream().map(v -> condition(v.visit(this)))
                .toArray(Condition[]::new);
        // Unlike and, if any OR elements are null we can't create a condition
        if(Arrays.stream(conditions).allMatch(Objects::nonNull)) {
            return DSL.or(conditions);
        } else {
            return null;
        }
    }

    private static <T> Field<T> field(final QueryPart part, final Class<T> type) {

        if(part == null) {
            return null;
        } else if(part instanceof Field<?>) {
            return cast((Field<?>) part, type);
        } else if(part instanceof Condition){
            return cast(DSL.field((Condition)part), type);
        } else {
            throw new IllegalStateException();
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> Field<T> cast(final Field<?> field, final Class<T> type) {

        if(type == Object.class) {
            return (Field<T>)field;
        } else {
            return field.cast(type);
        }
    }

    private static Condition condition(final QueryPart part) {

        if(part == null) {
            return null;
        } else if(part instanceof Field<?>) {
            return DSL.condition(((Field<?>)part).cast(Boolean.class));
        } else if(part instanceof Condition){
            return (Condition)part;
        } else {
            throw new IllegalStateException();
        }
    }

    public Condition condition(final Expression expression) {

        return condition(visit(expression));
    }
}
