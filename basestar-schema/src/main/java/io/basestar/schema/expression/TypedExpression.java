package io.basestar.schema.expression;

import io.basestar.expression.Expression;
import io.basestar.schema.use.Use;
import lombok.Data;

@Data
public class TypedExpression<V> {

    private final Expression expression;

    private final Use<V> type;

    public TypedExpression(final Expression expression, final Use<V> type) {

        this.expression = expression;
        this.type = type;
    }

    public static <V> TypedExpression<V> from(final Expression expression, final Use<V> type) {

        return new TypedExpression<>(expression, type);
    }
}
