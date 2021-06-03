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

import com.google.common.hash.Hashing;
import io.basestar.expression.parse.ExpressionCache;
import io.basestar.expression.type.Coercion;
import io.basestar.util.Immutable;
import io.basestar.util.Name;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

public interface Expression extends Serializable {

    Expression bind(Context context, Renaming root);

    default Expression bind(final Context context) {

        return bind(context, Renaming.noop());
    }

    Object evaluate(Context context);

    default boolean evaluatePredicate(final Context context) {

        return Coercion.isTruthy(evaluate(context));
    }

    // FIXME: this should try to coerce to the target type
    default <T> T evaluateAs(final Class<T> as, final Context context) {

        return as.cast(evaluate(context));
    }

    default boolean isAggregate() {

        return false;
    }

    default boolean hasAggregates() {

        return isAggregate() || expressions().stream().anyMatch(Expression::hasAggregates);
    }

    Set<Name> names();

    String token();

    int precedence();

    <T> T visit(ExpressionVisitor<T> visitor);

    default Set<String> closure() {

        return Collections.emptySet();
    }

    boolean isConstant(Closure closure);

    default boolean isConstant(final Set<String> closure) {

        return isConstant(Closure.from(closure));
    }

    default boolean isConstant() {

        return isConstant(Closure.none());
    }

    static Expression parse(final String expr) {

        return ExpressionCache.getDefault().parse(expr);
    }

    static Expression parseAndBind(final Context context, final String expr) {

        return parse(expr).bind(context);
    }

    List<Expression> expressions();

    Expression copy(List<Expression> expressions);

    // FIXME: drop expressions/copy and just use this method
    default Expression copy(final UnaryOperator<Expression> fn) {

        return copy(expressions().stream().map(fn).collect(Collectors.toList()));
    }

    @SuppressWarnings("UnstableApiUsage")
    default String digest() {

        return Hashing.murmur3_32().hashString(toString(), StandardCharsets.UTF_8).toString();
    }

    interface Closure extends Serializable {

        boolean has(String name);

        static Closure none() {

            return name -> false;
        }

        static Closure from(final Set<String> closure) {

            final Set<String> copy = Immutable.set(closure);
            return copy::contains;
        }

        default Closure with(final Set<String> closure) {

            final Set<String> copy = Immutable.set(closure);
            return name -> copy.contains(name) || Closure.this.has(name);
        }
    }
}
