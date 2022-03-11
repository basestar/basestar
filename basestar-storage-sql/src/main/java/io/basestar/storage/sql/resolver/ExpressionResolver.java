package io.basestar.storage.sql.resolver;

import io.basestar.expression.Expression;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.util.Warnings;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.QueryPart;
import org.jooq.impl.DSL;

public interface ExpressionResolver {

    QueryPart expression(ColumnResolver columnResolver, InferenceContext inferenceContext, Expression expression);

    @SuppressWarnings(Warnings.RETURN_GENERIC_WILDCARD)
    default Field<?> field(final ColumnResolver columnResolver, final InferenceContext inferenceContext, final Expression expression) {

        final QueryPart part = expression(columnResolver, inferenceContext, expression);
        if (part == null) {
            return null;
        } else if (part instanceof Field<?>) {
            return (Field<?>) part;
        } else if (part instanceof Condition) {
            return DSL.field((Condition) part);
        } else {
            throw new IllegalStateException();
        }
    }

    default Condition condition(final ColumnResolver columnResolver, final InferenceContext inferenceContext, final Expression expression) {

        final QueryPart part = expression(columnResolver, inferenceContext, expression);
        if (part == null) {
            return null;
        } else if (part instanceof Field<?>) {
            final Field<?> field = (Field<?>) part;
            return field.isNotNull().and(field.cast(Boolean.class));
        } else if (part instanceof Condition) {
            return (Condition) part;
        } else {
            throw new IllegalStateException();
        }
    }
}
