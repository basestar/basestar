package io.basestar.expression;

/*-
 * #%L
 * basestar-expression
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

import io.basestar.expression.aggregate.*;
import io.basestar.expression.arithmetic.*;
import io.basestar.expression.bitwise.*;
import io.basestar.expression.call.LambdaCall;
import io.basestar.expression.call.MemberCall;
import io.basestar.expression.compare.*;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.constant.NameConstant;
import io.basestar.expression.function.*;
import io.basestar.expression.iterate.*;
import io.basestar.expression.literal.LiteralArray;
import io.basestar.expression.literal.LiteralObject;
import io.basestar.expression.literal.LiteralSet;
import io.basestar.expression.logical.And;
import io.basestar.expression.logical.Not;
import io.basestar.expression.logical.Or;
import io.basestar.expression.sql.Sql;
import io.basestar.expression.text.Like;

public interface ExpressionVisitor<T> {

    default T visit(final Expression expression) {

        return expression.visit(this);
    }

    T visitAdd(Add expression);

    T visitDiv(Div expression);

    T visitMod(Mod expression);

    T visitMul(Mul expression);

    T visitNegate(Negate expression);

    T visitPow(Pow expression);

    T visitSub(Sub expression);

    T visitBitAnd(BitAnd expression);

    T visitBitFlip(BitNot expression);

    T visitBitLsh(BitLsh expression);

    T visitBitOr(BitOr expression);

    T visitBitRsh(BitRsh expression);

    T visitBitXor(BitXor expression);

    T visitCmp(Cmp expression);

    T visitEq(Eq expression);

    T visitGt(Gt expression);

    T visitGte(Gte expression);

    T visitLt(Lt expression);

    T visitLte(Lte expression);

    T visitNe(Ne expression);

    T visitConstant(Constant expression);

    T visitNameConstant(NameConstant expression);

    T visitCoalesce(Coalesce expression);

    T visitIfElse(IfElse expression);

    T visitIn(In expression);

    T visitIndex(Index expression);

    T visitLambda(Lambda expression);

    T visitLambdaCall(LambdaCall expression);

    T visitMemberCall(MemberCall expression);

    T visitMember(Member expression);

    T visitWith(With expression);

    T visitForAll(ForAll expression);

    T visitForAny(ForAny expression);

    T visitForArray(ForArray expression);

    T visitForObject(ForObject expression);

    T visitForSet(ForSet expression);

    T visitLiteralArray(LiteralArray expression);

    T visitLiteralObject(LiteralObject expression);

    T visitLiteralSet(LiteralSet expression);

    T visitAnd(And expression);

    T visitNot(Not expression);

    T visitOr(Or expression);

    T visitLike(Like expression);

    T visitAvg(Avg expression);

    T visitCollectArray(CollectArray expression);

    T visitCount(Count expression);

    T visitMax(Max expression);

    T visitMin(Min expression);

    T visitSum(Sum expression);

    T visitSql(Sql expression);

    T visitCast(Cast expression);

    T visitCase(Case expression);

    interface Defaulting<T> extends ExpressionVisitor<T> {

        T visitDefault(Expression expression);

        default T visitAggregate(Aggregate expression) {

            return visitDefault(expression);
        }

        @Override
        default T visitAdd(final Add expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitDiv(final Div expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitMod(final Mod expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitMul(final Mul expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitNegate(final Negate expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitPow(final Pow expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitSub(final Sub expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitBitAnd(final BitAnd expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitBitFlip(final BitNot expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitBitLsh(final BitLsh expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitBitOr(final BitOr expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitBitRsh(final BitRsh expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitBitXor(final BitXor expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitCmp(final Cmp expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitEq(final Eq expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitGt(final Gt expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitGte(final Gte expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitLt(final Lt expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitLte(final Lte expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitNe(final Ne expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitConstant(final Constant expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitNameConstant(final NameConstant expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitCoalesce(final Coalesce expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitIfElse(final IfElse expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitIn(final In expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitIndex(final Index expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitLambda(final Lambda expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitLambdaCall(final LambdaCall expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitMemberCall(final MemberCall expression) {

            return visitDefault(expression);
        }

        @Override
        default T visitMember(final Member expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitWith(final With expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitForAll(final ForAll expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitForAny(final ForAny expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitForArray(final ForArray expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitForObject(final ForObject expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitForSet(final ForSet expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitLiteralArray(final LiteralArray expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitLiteralObject(final LiteralObject expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitLiteralSet(final LiteralSet expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitAnd(final And expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitNot(final Not expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitOr(final Or expression) {
            
            return visitDefault(expression);
        }

        @Override
        default T visitLike(final Like expression) {

            return visitDefault(expression);
        }

        @Override
        default T visitAvg(final Avg expression) {

            return visitAggregate(expression);
        }

        @Override
        default T visitCollectArray(final CollectArray expression) {

            return visitAggregate(expression);
        }

        @Override
        default T visitCount(final Count expression) {

            return visitAggregate(expression);
        }

        @Override
        default T visitMax(final Max expression) {

            return visitAggregate(expression);
        }

        @Override
        default T visitMin(final Min expression) {

            return visitAggregate(expression);
        }

        @Override
        default T visitSum(final Sum expression) {

            return visitAggregate(expression);
        }

        @Override
        default T visitSql(final Sql expression) {

            return visitDefault(expression);
        }

        @Override
        default T visitCast(final Cast expression) {

            return visitDefault(expression);
        }

        @Override
        default T visitCase(final Case expression) {

            return visitDefault(expression);
        }
    }
}
