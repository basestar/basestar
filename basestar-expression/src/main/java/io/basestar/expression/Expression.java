package io.basestar.expression;

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

import io.basestar.expression.parse.ExpressionCache;
import io.basestar.expression.type.Values;
import io.basestar.util.Path;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;

public interface Expression extends Serializable {

    Expression bind(Context context, PathTransform root);

    default Expression bind(Context context) {

        return bind(context, PathTransform.noop());
    }

    Object evaluate(Context context);

    default boolean evaluatePredicate(final Context context) {

        return Values.isTruthy(evaluate(context));
    }

    //    Query query();

    Set<Path> paths();

    String token();

    int precedence();

    <T> T visit(ExpressionVisitor<T> visitor);

    default Set<String> closure() {

        return Collections.emptySet();
    }

    static Expression parse(final String expr) {

        return ExpressionCache.getDefault().parse(expr);
    }

    //    Set<And> disjunction();
//
//    Map<Path, Object> constants();
}
