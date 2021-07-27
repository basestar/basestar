package io.basestar.schema.from;

import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.logical.And;
import io.basestar.util.Pair;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class JoinExpressionVisitor implements ExpressionVisitor.Defaulting<Pair<Expression, Expression>> {

    private final Expression.Closure closure;

    @Override
    public Pair<Expression, Expression> visitDefault(final Expression expression) {

        if (expression.isConstant(closure)) {
            return Pair.of(expression, Constant.TRUE);
        } else {
            return Pair.of(Constant.TRUE, expression);
        }
    }

    @Override
    public Pair<Expression, Expression> visitAnd(final And expression) {

        return mergeSides(expression);
    }

    private Pair<Expression, Expression> mergeSides(final Expression expression) {

        final List<Pair<Expression, Expression>> sides = expression.expressions().stream()
                .map(this::visit).collect(Collectors.toList());
        return Pair.of(
                expression.copy(sides.stream().map(Pair::getFirst).collect(Collectors.toList())),
                expression.copy(sides.stream().map(Pair::getSecond).collect(Collectors.toList()))
        );
    }

    public Pair<Expression, Expression> visitAndSimplify(final Expression expr) {

        final Pair<Expression, Expression> result = visit(expr);
        return Pair.of(result.getFirst().bind(Context.init()), result.getSecond().bind(Context.init()));
    }
}
