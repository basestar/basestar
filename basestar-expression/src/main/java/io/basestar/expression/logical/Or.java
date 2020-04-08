package io.basestar.expression.logical;

/*-
 * #%L
 * basestar-expression
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

import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.PathTransform;
import io.basestar.expression.constant.Constant;
import io.basestar.util.Path;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Or
 *
 * Logical Disjunction
 */

@Data
@AllArgsConstructor
public class Or implements Expression {

    public static final String TOKEN = "||";

    public static final int PRECEDENCE = And.PRECEDENCE + 1;

    private final List<Expression> terms;

    public Or(final Expression ... terms) {

        this.terms = Arrays.asList(terms);
    }

    /**
     * lhs || rhs
     *
     * @param lhs any Left hand operand
     * @param rhs any Right hand operand
     */

    public Or(final Expression lhs, final Expression rhs) {

        this.terms = Arrays.asList(lhs, rhs);
    }

    @Override
    public Expression bind(final Context context, final PathTransform root) {

        boolean changed = false;
        boolean constant = true;
        final List<Expression> terms = this.terms.stream().map(v -> v.bind(context, root)).collect(Collectors.toList());
        for(int i = 0; i != terms.size(); ++i) {
            final Expression before = this.terms.get(i);
            final Expression after = terms.get(i);
            changed = changed || before != after;
            constant = constant && after instanceof Constant;
        }
        if(constant) {
            return new Constant(new Or(terms).evaluate(context));
        } else if(changed) {
            return new Or(terms);
        } else {
            return this;
        }
    }

    @Override
    public Boolean evaluate(final Context context) {

        if(terms.isEmpty()) {
            return false;
        } else {
            return terms.stream().anyMatch(v -> v.evaluatePredicate(context));
//            Object prev = terms.get(0).evaluate(context);
//            if(Values.isTruthy(prev)) {
//                return prev;
//            } else {
//                for (final Expression expr : terms.subList(1, terms.size())) {
//                    final Object next = expr.evaluate(context);
//                    if (Values.isTruthy(next)) {
//                        return next;
//                    } else {
//                        prev = next;
//                    }
//                }
//                return prev;
//            }
        }
    }

//    @Override
//    public Query query() {
//
//        return Query.and();
//    }

    @Override
    public Set<Path> paths() {

        return terms.stream().flatMap(v -> v.paths().stream())
                .collect(Collectors.toSet());
    }

    @Override
    public String token() {

        return TOKEN;
    }

    @Override
    public int precedence() {

        return PRECEDENCE;
    }

    @Override
    public <T> T visit(final ExpressionVisitor<T> visitor) {

        return visitor.visitOr(this);
    }

    @Override
    public String toString() {

        return terms.stream().map(v -> {
            if(v.precedence() > precedence()) {
                return "(" + v + ")";
            } else {
                return v.toString();
            }
        }).collect(Collectors.joining(" " + TOKEN + " "));
    }
}
