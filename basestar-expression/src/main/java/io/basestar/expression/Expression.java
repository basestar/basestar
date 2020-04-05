package io.basestar.expression;

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
