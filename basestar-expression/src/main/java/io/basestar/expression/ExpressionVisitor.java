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

    T visitPathConstant(PathConstant expression);

    T visitCoalesce(Coalesce expression);

    T visitIfElse(IfElse expression);

    T visitIn(In expression);

    T visitIndex(Index expression);

    T visitLambda(Lambda expression);

    T visitLambdaCall(LambdaCall expression);

    T visitMember(Member expression);

    T visitMemberCall(MemberCall expression);

    T visitStarMember(StarMember expression);

    T visitWith(With expression);

    T visitForAll(ForAll expression);

    T visitForAny(ForAny expression);

    T visitForArray(ForArray expression);

    T visitForObject(ForObject expression);

    T visitForSet(ForSet expression);

    T visitOf(Of expression);

    T visitWhere(Where expression);

    T visitLiteralArray(LiteralArray expression);

    T visitLiteralObject(LiteralObject expression);

    T visitLiteralSet(LiteralSet expression);

    T visitAnd(And expression);

    T visitNot(Not expression);

    T visitOr(Or expression);

    interface Defaulting<T> extends ExpressionVisitor<T> {

        T visitDefault(Expression expression);

        default T visitAdd(final Add expression) {
            
            return visitDefault(expression);
        }

        default T visitDiv(final Div expression){
            
            return visitDefault(expression);
        }

        default T visitMod(final Mod expression){
            
            return visitDefault(expression);
        }

        default T visitMul(final Mul expression){
            
            return visitDefault(expression);
        }

        default T visitNegate(final Negate expression){
            
            return visitDefault(expression);
        }

        default T visitPow(final Pow expression){
            
            return visitDefault(expression);
        }

        default T visitSub(final Sub expression){
            
            return visitDefault(expression);
        }

        default T visitBitAnd(final BitAnd expression){
            
            return visitDefault(expression);
        }

        default T visitBitFlip(final BitNot expression){
            
            return visitDefault(expression);
        }

        default T visitBitLsh(final BitLsh expression){
            
            return visitDefault(expression);
        }

        default T visitBitOr(final BitOr expression){
            
            return visitDefault(expression);
        }

        default T visitBitRsh(final BitRsh expression){
            
            return visitDefault(expression);
        }

        default T visitBitXor(final BitXor expression){
            
            return visitDefault(expression);
        }

        default T visitCmp(final Cmp expression){
            
            return visitDefault(expression);
        }

        default T visitEq(final Eq expression){
            
            return visitDefault(expression);
        }

        default T visitGt(final Gt expression){
            
            return visitDefault(expression);
        }

        default T visitGte(final Gte expression){
            
            return visitDefault(expression);
        }

        default T visitLt(final Lt expression){
            
            return visitDefault(expression);
        }

        default T visitLte(final Lte expression){
            
            return visitDefault(expression);
        }

        default T visitNe(final Ne expression){
            
            return visitDefault(expression);
        }

        default T visitConstant(final Constant expression){
            
            return visitDefault(expression);
        }

        default T visitPathConstant(final PathConstant expression){
            
            return visitDefault(expression);
        }

        default T visitCoalesce(final Coalesce expression){
            
            return visitDefault(expression);
        }

        default T visitIfElse(final IfElse expression){
            
            return visitDefault(expression);
        }

        default T visitIn(final In expression){
            
            return visitDefault(expression);
        }

        default T visitIndex(final Index expression){
            
            return visitDefault(expression);
        }

        default T visitLambda(final Lambda expression){
            
            return visitDefault(expression);
        }

        default T visitLambdaCall(final LambdaCall expression){
            
            return visitDefault(expression);
        }

        default T visitMember(final Member expression){
            
            return visitDefault(expression);
        }

        default T visitMemberCall(final MemberCall expression){
            
            return visitDefault(expression);
        }

        default T visitStarMember(final StarMember expression){
            
            return visitDefault(expression);
        }

        default T visitWith(final With expression){
            
            return visitDefault(expression);
        }

        default T visitForAll(final ForAll expression){
            
            return visitDefault(expression);
        }

        default T visitForAny(final ForAny expression){
            
            return visitDefault(expression);
        }

        default T visitForArray(final ForArray expression){
            
            return visitDefault(expression);
        }

        default T visitForObject(final ForObject expression){
            
            return visitDefault(expression);
        }

        default T visitForSet(final ForSet expression){
            
            return visitDefault(expression);
        }

        default T visitOf(final Of expression){
            
            return visitDefault(expression);
        }

        default T visitWhere(final Where expression){
            
            return visitDefault(expression);
        }

        default T visitLiteralArray(final LiteralArray expression){
            
            return visitDefault(expression);
        }

        default T visitLiteralObject(final LiteralObject expression){
            
            return visitDefault(expression);
        }

        default T visitLiteralSet(final LiteralSet expression){
            
            return visitDefault(expression);
        }

        default T visitAnd(final And expression){
            
            return visitDefault(expression);
        }

        default T visitNot(final Not expression){
            
            return visitDefault(expression);
        }

        default T visitOr(final Or expression){
            
            return visitDefault(expression);
        }
    }
}
