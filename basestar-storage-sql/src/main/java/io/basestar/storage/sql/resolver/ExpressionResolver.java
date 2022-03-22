package io.basestar.storage.sql.resolver;

import io.basestar.expression.Expression;
import io.basestar.storage.sql.mapping.QueryMapping;
import io.basestar.util.Warnings;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.QueryPart;
import org.jooq.impl.DSL;

public interface ExpressionResolver {

    QueryPart expression(DSLContext context, TableResolver tableResolver, ColumnResolver columnResolver, QueryMapping queryMapping, Expression expression);

    @SuppressWarnings(Warnings.RETURN_GENERIC_WILDCARD)
    default Field<?> field(final DSLContext context, final TableResolver tableResolver, final ColumnResolver columnResolver, final QueryMapping queryMapping, final Expression expression) {

        final QueryPart part = expression(context, tableResolver, columnResolver, queryMapping, expression);
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

    default Condition condition(final DSLContext context, final TableResolver tableResolver, final ColumnResolver columnResolver, final QueryMapping queryMapping, final Expression expression) {

        final QueryPart part = expression(context, tableResolver, columnResolver, queryMapping, expression);
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
