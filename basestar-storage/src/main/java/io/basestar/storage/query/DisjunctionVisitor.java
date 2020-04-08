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

import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.compare.Eq;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.constant.PathConstant;
import io.basestar.expression.function.In;
import io.basestar.expression.iterate.ForAny;
import io.basestar.expression.logical.And;
import io.basestar.expression.logical.Or;
import io.basestar.util.Path;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Pull all top-level logical OR constructs to top level
 */

public class DisjunctionVisitor implements ExpressionVisitor.Defaulting<Set<Expression>> {

    @Override
    public Set<Expression> visitDefault(final Expression expression) {

        return Collections.singleton(expression);
    }

    @Override
    public Set<Expression> visitAnd(final And expression) {

        return all(expression.getTerms().stream().map(this::visit)
                .collect(Collectors.toList()));
    }

    @Override
    public Set<Expression> visitOr(final Or expression) {

        return expression.getTerms().stream().flatMap(v -> visit(v).stream())
                .collect(Collectors.toSet());
    }

    @Override
    public Set<Expression> visitIn(final In expression) {

        final Expression lhs = expression.getLhs();
        final Expression rhs = expression.getRhs();
        if(lhs instanceof PathConstant && rhs instanceof Constant) {
            final Path path = ((PathConstant)lhs).getPath();
            final Object value = ((Constant)rhs).getValue();
            if(value instanceof Collection<?>) {
                return ((Collection<?>) value).stream()
                        .map(v -> new Eq(new PathConstant(path), new Constant(value)))
                        .collect(Collectors.toSet());
            } else {
                return Collections.singleton(expression);
            }
        } else {
            return Collections.singleton(expression);
        }
    }

    @Override
    public Set<Expression> visitForAny(final ForAny expression) {

        final Expression lhs = expression.getLhs();
        final Expression rhs = expression.getRhs();
        final Set<Expression> terms = visit(lhs);
        return terms.stream()
                .map(term -> new ForAny(term, rhs))
                .collect(Collectors.toSet());
    }

    private Set<Expression> all(final List<Set<Expression>> rest) {

        if(rest.isEmpty()) {
            return Collections.singleton(new And());
        } else {
            final Set<Expression> result = new HashSet<>();
            final Set<Expression> head = rest.get(0);
            for(final Expression a : head) {
                for(final Expression b : all(rest.subList(1, rest.size()))) {
                    result.add(merge(a, b));
                }
            }
            return result;
        }
    }

    private Expression merge(final Expression a, final Expression b) {

        final List<Expression> terms = new ArrayList<>();
        if(a instanceof And) {
            terms.addAll(((And) a).getTerms());
        } else {
            terms.add(a);
        }
        if(b instanceof And) {
            terms.addAll(((And) b).getTerms());
        } else {
            terms.add(b);
        }
        return new And(terms);
    }
}
