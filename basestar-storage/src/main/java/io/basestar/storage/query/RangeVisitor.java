package io.basestar.storage.query;

/*-
 * #%L
 * basestar-storage
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

import com.google.common.collect.ImmutableMap;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.compare.*;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.constant.NameConstant;
import io.basestar.expression.iterate.ForAny;
import io.basestar.expression.iterate.Of;
import io.basestar.expression.logical.And;
import io.basestar.expression.type.Values;
import io.basestar.util.Name;

import java.util.HashMap;
import java.util.Map;

public class RangeVisitor implements ExpressionVisitor.Defaulting<Map<Name, Range<Object>>> {

    @Override
    public Map<Name, Range<Object>> visitDefault(final Expression expression) {

        return ImmutableMap.of();
    }

    public Map<Name, Range<Object>> visitEq(final Eq expression) {

        final Expression lhs = expression.getLhs();
        final Expression rhs = expression.getRhs();
        if (lhs instanceof NameConstant && rhs instanceof Constant) {
            return ImmutableMap.of(((NameConstant) lhs).getName(), Range.eq(((Constant) rhs).getValue()));
        } else if (lhs instanceof Constant && rhs instanceof NameConstant) {
            return ImmutableMap.of(((NameConstant) rhs).getName(), Range.eq(((Constant) lhs).getValue()));
        } else {
            return ImmutableMap.of();
        }
    }

    @Override
    public Map<Name, Range<Object>> visitLt(final Lt expression) {

        final Expression lhs = expression.getLhs();
        final Expression rhs = expression.getRhs();
        if (lhs instanceof NameConstant && rhs instanceof Constant) {
            return ImmutableMap.of(((NameConstant) lhs).getName(), Range.lt(((Constant) rhs).getValue()));
        } else if (lhs instanceof Constant && rhs instanceof NameConstant) {
            return ImmutableMap.of(((NameConstant) rhs).getName(), Range.gte(((Constant) lhs).getValue()));
        } else {
            return ImmutableMap.of();
        }
    }

    @Override
    public Map<Name, Range<Object>> visitLte(final Lte expression) {

        final Expression lhs = expression.getLhs();
        final Expression rhs = expression.getRhs();
        if (lhs instanceof NameConstant && rhs instanceof Constant) {
            return ImmutableMap.of(((NameConstant) lhs).getName(), Range.lte(((Constant) rhs).getValue()));
        } else if (lhs instanceof Constant && rhs instanceof NameConstant) {
            return ImmutableMap.of(((NameConstant) rhs).getName(), Range.gt(((Constant) lhs).getValue()));
        } else {
            return ImmutableMap.of();
        }
    }

    @Override
    public Map<Name, Range<Object>> visitGt(final Gt expression) {

        final Expression lhs = expression.getLhs();
        final Expression rhs = expression.getRhs();
        if (lhs instanceof NameConstant && rhs instanceof Constant) {
            return ImmutableMap.of(((NameConstant) lhs).getName(), Range.gt(((Constant) rhs).getValue()));
        } else if (lhs instanceof Constant && rhs instanceof NameConstant) {
            return ImmutableMap.of(((NameConstant) rhs).getName(), Range.lte(((Constant) lhs).getValue()));
        } else {
            return ImmutableMap.of();
        }
    }

    @Override
    public Map<Name, Range<Object>> visitGte(final Gte expression) {

        final Expression lhs = expression.getLhs();
        final Expression rhs = expression.getRhs();
        if (lhs instanceof NameConstant && rhs instanceof Constant) {
            return ImmutableMap.of(((NameConstant) lhs).getName(), Range.gte(((Constant) rhs).getValue()));
        } else if (lhs instanceof Constant && rhs instanceof NameConstant) {
            return ImmutableMap.of(((NameConstant) rhs).getName(), Range.lt(((Constant) lhs).getValue()));
        } else {
            return ImmutableMap.of();
        }
    }

    @Override
    public Map<Name, Range<Object>> visitAnd(final And expression) {

        final Map<Name, Range<Object>> result = new HashMap<>();
        for(final Expression term : expression.getTerms()) {
            for (final Map.Entry<Name, Range<Object>> entry : visit(term).entrySet()) {
                final Name name = entry.getKey();
                final Range<Object> range = entry.getValue();
                if (result.containsKey(name)) {
                    result.put(name, Range.and(result.get(name), range, Values::compare));
                } else {
                    result.put(name, range);
                }
            }
        }
        return result;
    }

    // FIXME: wont work when 2 for any are in the same disjunction branch

    @Override
    public Map<Name, Range<Object>> visitForAny(final ForAny expression) {

        final Expression lhs = expression.getLhs();
        final Expression rhs = expression.getRhs();
        if(rhs instanceof Of) {
            final Of of = (Of)rhs;
            if(of.getExpr() instanceof NameConstant) {
                final Name name = ((NameConstant) of.getExpr()).getName();
                // map keys not supported
                if(of.getKey() == null) {
                    final String value = of.getValue();
                    final Map<Name, Range<Object>> lhsRange = lhs.visit(this);
                    final Map<Name, Range<Object>> results = new HashMap<>();
                    lhsRange.forEach((k, v) -> {
                        if(k.first().equals(value)) {
                            results.put(name.with(k.withoutFirst()), v);
                        }
                    });
                    return results;
                }
            }
        }
        return ImmutableMap.of();
    }
}
