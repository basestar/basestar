package io.basestar.storage.hazelcast;

/*-
 * #%L
 * basestar-storage-hazelcast
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2020 basestar.io
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

import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.predicates.*;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.compare.*;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.constant.PathConstant;
import io.basestar.expression.logical.And;
import io.basestar.expression.logical.Not;
import io.basestar.expression.logical.Or;
import io.basestar.expression.type.Values;

// FIXME more predicates can be implemented
public class HazelcastExpressionVisitor<K, V> implements ExpressionVisitor.Defaulting<Predicate<K, V>> {

    @Override
    @SuppressWarnings("unchecked")
    public Predicate<K, V> visitDefault(final Expression expression) {

        return TruePredicate.INSTANCE;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Predicate<K, V> visitEq(final Eq expression) {

        final Expression lhs = expression.getLhs();
        final Expression rhs = expression.getRhs();
        if (lhs instanceof PathConstant && rhs instanceof Constant) {
            return new EqualPredicate(
                    ((PathConstant) lhs).getPath().toString(),
                    (Comparable)((Constant) rhs).getValue()
            );
        } else if (lhs instanceof Constant && rhs instanceof PathConstant) {
            return new EqualPredicate(
                    ((PathConstant) rhs).getPath().toString(),
                    (Comparable)((Constant) lhs).getValue()
            );
        } else {
            return null;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Predicate<K, V> visitNe(final Ne expression) {

        final Expression lhs = expression.getLhs();
        final Expression rhs = expression.getRhs();
        if (lhs instanceof PathConstant && rhs instanceof Constant) {
            return new NotEqualPredicate(
                    ((PathConstant) lhs).getPath().toString(),
                    (Comparable)((Constant) rhs).getValue()
            );
        } else if (lhs instanceof Constant && rhs instanceof PathConstant) {
            return new NotEqualPredicate(
                    ((PathConstant) rhs).getPath().toString(),
                    (Comparable)((Constant) lhs).getValue()
            );
        } else {
            return null;
        }
    }

    @Override
    public Predicate<K, V> visitGt(final Gt expression) {

        return visitGl(expression.getLhs(), expression.getRhs(), false, false);
    }

    @Override
    public Predicate<K, V> visitGte(final Gte expression) {

        return visitGl(expression.getLhs(), expression.getRhs(), true, false);
    }

    @Override
    public Predicate<K, V> visitLt(final Lt expression) {

        return visitGl(expression.getLhs(), expression.getRhs(), false, true);
    }

    @Override
    public Predicate<K, V> visitLte(final Lte expression) {

        return visitGl(expression.getLhs(), expression.getRhs(), true, true);
    }

    @SuppressWarnings("unchecked")
    private Predicate<K, V> visitGl(final Expression lhs, final Expression rhs, final boolean equal, final boolean less) {

        if (lhs instanceof PathConstant && rhs instanceof Constant) {
            return new GreaterLessPredicate(
                    ((PathConstant) lhs).getPath().toString(),
                    (Comparable)((Constant) rhs).getValue(),
                    equal, less
            );
        } else if (lhs instanceof Constant && rhs instanceof PathConstant) {
            return new GreaterLessPredicate(
                    ((PathConstant) rhs).getPath().toString(),
                    (Comparable)((Constant) lhs).getValue(),
                    !equal, !less
            );
        } else {
            return null;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Predicate<K, V> visitConstant(final Constant expression) {

        return Values.isTruthy(expression.getValue()) ? TruePredicate.INSTANCE : FalsePredicate.INSTANCE;
    }


    @Override
    @SuppressWarnings("unchecked")
    public Predicate<K, V> visitAnd(final And expression) {

        return new AndPredicate(expression.getTerms().stream().map(v -> v.visit(this))
                .toArray(Predicate<?, ?>[]::new));
    }

    @Override
    @SuppressWarnings("unchecked")
    public Predicate<K, V> visitNot(final Not expression) {

        return new NotPredicate(expression.getOperand().visit(this));
    }

    @Override
    @SuppressWarnings("unchecked")
    public Predicate<K, V> visitOr(final Or expression) {

        return new OrPredicate(expression.getTerms().stream().map(v -> v.visit(this))
                .toArray(Predicate<?, ?>[]::new));
    }
}
