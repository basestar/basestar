package io.basestar.storage.query;

/*-
 * #%L
 * basestar-storage
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

import com.google.common.collect.ImmutableMap;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.compare.*;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.constant.PathConstant;
import io.basestar.expression.iterate.ForAny;
import io.basestar.expression.iterate.Of;
import io.basestar.expression.logical.And;
import io.basestar.expression.type.Values;
import io.basestar.util.Path;

import java.util.HashMap;
import java.util.Map;

public class RangeVisitor implements ExpressionVisitor.Defaulting<Map<Path, Range<Object>>> {

    @Override
    public Map<Path, Range<Object>> visitDefault(final Expression expression) {

        return ImmutableMap.of();
    }

    public Map<Path, Range<Object>> visitEq(final Eq expression) {

        final Expression lhs = expression.getLhs();
        final Expression rhs = expression.getRhs();
        if (lhs instanceof PathConstant && rhs instanceof Constant) {
            return ImmutableMap.of(((PathConstant) lhs).getPath(), Range.eq(((Constant) rhs).getValue()));
        } else if (lhs instanceof Constant && rhs instanceof PathConstant) {
            return ImmutableMap.of(((PathConstant) rhs).getPath(), Range.eq(((Constant) lhs).getValue()));
        } else {
            return ImmutableMap.of();
        }
    }

    @Override
    public Map<Path, Range<Object>> visitLt(final Lt expression) {

        final Expression lhs = expression.getLhs();
        final Expression rhs = expression.getRhs();
        if (lhs instanceof PathConstant && rhs instanceof Constant) {
            return ImmutableMap.of(((PathConstant) lhs).getPath(), Range.lt(((Constant) rhs).getValue()));
        } else if (lhs instanceof Constant && rhs instanceof PathConstant) {
            return ImmutableMap.of(((PathConstant) rhs).getPath(), Range.gte(((Constant) lhs).getValue()));
        } else {
            return ImmutableMap.of();
        }
    }

    @Override
    public Map<Path, Range<Object>> visitLte(final Lte expression) {

        final Expression lhs = expression.getLhs();
        final Expression rhs = expression.getRhs();
        if (lhs instanceof PathConstant && rhs instanceof Constant) {
            return ImmutableMap.of(((PathConstant) lhs).getPath(), Range.lte(((Constant) rhs).getValue()));
        } else if (lhs instanceof Constant && rhs instanceof PathConstant) {
            return ImmutableMap.of(((PathConstant) rhs).getPath(), Range.gt(((Constant) lhs).getValue()));
        } else {
            return ImmutableMap.of();
        }
    }

    @Override
    public Map<Path, Range<Object>> visitGt(final Gt expression) {

        final Expression lhs = expression.getLhs();
        final Expression rhs = expression.getRhs();
        if (lhs instanceof PathConstant && rhs instanceof Constant) {
            return ImmutableMap.of(((PathConstant) lhs).getPath(), Range.gt(((Constant) rhs).getValue()));
        } else if (lhs instanceof Constant && rhs instanceof PathConstant) {
            return ImmutableMap.of(((PathConstant) rhs).getPath(), Range.lte(((Constant) lhs).getValue()));
        } else {
            return ImmutableMap.of();
        }
    }

    @Override
    public Map<Path, Range<Object>> visitGte(final Gte expression) {

        final Expression lhs = expression.getLhs();
        final Expression rhs = expression.getRhs();
        if (lhs instanceof PathConstant && rhs instanceof Constant) {
            return ImmutableMap.of(((PathConstant) lhs).getPath(), Range.gte(((Constant) rhs).getValue()));
        } else if (lhs instanceof Constant && rhs instanceof PathConstant) {
            return ImmutableMap.of(((PathConstant) rhs).getPath(), Range.lt(((Constant) lhs).getValue()));
        } else {
            return ImmutableMap.of();
        }
    }

    @Override
    public Map<Path, Range<Object>> visitAnd(final And expression) {

        final Map<Path, Range<Object>> result = new HashMap<>();
        for(final Expression term : expression.getTerms()) {
            for (final Map.Entry<Path, Range<Object>> entry : visit(term).entrySet()) {
                final Path path = entry.getKey();
                final Range<Object> range = entry.getValue();
                if (result.containsKey(path)) {
                    result.put(path, Range.and(result.get(path), range, Values::compare));
                } else {
                    result.put(path, range);
                }
            }
        }
        return result;
    }

    // FIXME: wont work when 2 for any are in the same disjunction branch

    @Override
    public Map<Path, Range<Object>> visitForAny(final ForAny expression) {

        final Expression lhs = expression.getLhs();
        final Expression rhs = expression.getRhs();
        if(rhs instanceof Of) {
            final Of of = (Of)rhs;
            if(of.getExpr() instanceof PathConstant) {
                final Path path = ((PathConstant) of.getExpr()).getPath();
                // map keys not supported
                if(of.getKey() == null) {
                    final String value = of.getValue();
                    final Map<Path, Range<Object>> lhsRange = lhs.visit(this);
                    final Map<Path, Range<Object>> results = new HashMap<>();
                    lhsRange.forEach((k, v) -> {
                        if(k.first().equals(value)) {
                            results.put(path.with(k.withoutFirst()), v);
                        }
                    });
                    return results;
                }
            }
        }
        return ImmutableMap.of();
    }
}
