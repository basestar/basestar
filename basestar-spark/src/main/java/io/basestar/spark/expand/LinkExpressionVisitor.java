package io.basestar.spark.expand;

import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.constant.NameConstant;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class LinkExpressionVisitor implements ExpressionVisitor.Defaulting<Expression> {

    private final String prefix;

    private final Set<String> closure;

    @Getter
    private final Map<String, Expression> constants = new HashMap<>();

    @Override
    public Expression visitDefault(final Expression expression) {

        if(expression.isConstant(closure)) {
            final String id = prefix + expression.digest();
            constants.put(id, expression);
            return new NameConstant(id);
        } else {
            return expression.copy(expression.expressions().stream()
                    .map(this::visit).collect(Collectors.toList()));
        }
    }
}
