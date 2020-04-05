package io.basestar.expression;

import com.google.common.collect.ImmutableSet;
import io.basestar.expression.constant.Constant;
import io.basestar.util.Path;

import java.util.Set;

public interface Binary extends Expression {

    Expression getLhs();

    Expression getRhs();

    Expression create(Expression lhs, Expression rhs);

    @Override
    default Set<Path> paths() {

        return ImmutableSet.<Path>builder()
                .addAll(getLhs().paths())
                .addAll(getRhs().paths())
                .build();
    }

    default String toString(final Expression lhs, final Expression rhs) {

        final StringBuilder str = new StringBuilder();
        if(lhs.precedence() > precedence()) {
            str.append("(");
            str.append(lhs);
            str.append(")");
        } else {
            str.append(lhs);
        }
        str.append(" ");
        str.append(token());
        str.append(" ");
        if(rhs.precedence() > precedence()) {
            str.append("(");
            str.append(rhs);
            str.append(")");
        } else {
            str.append(rhs);
        }
        return str.toString();
    }

//    @Override
//    default String toString(final int precedence) {
//
//        return "(" + getLhs().toString(precedence) + token() + getRhs().toString(precedence) + ")";
//    }

    @Override
    default Expression bind(final Context context, final PathTransform root) {

        final Expression beforeLhs = getLhs();
        final Expression beforeRhs = getRhs();
        final Expression afterLhs = bindLhs(context, root);
        final Expression afterRhs = bindRhs(context, root);
        if(afterLhs instanceof Constant && afterRhs instanceof Constant) {
            return new Constant(create(afterLhs, afterRhs).evaluate(context));
        } else {
            return beforeLhs == afterLhs && beforeRhs == afterRhs ? this : create(afterLhs, afterRhs);
        }
    }

    default Expression bindLhs(final Context context, final PathTransform root) {

        return getLhs().bind(context, root);
    }

    default Expression bindRhs(final Context context, final PathTransform root) {

        return getRhs().bind(context, root);
    }
}
