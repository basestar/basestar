package io.basestar.storage.sql.dialect;

import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.storage.sql.SQLExpressionVisitor;
import io.basestar.util.Immutable;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestSnowflakeDialect {

    private SQLExpressionVisitor visitor() {

        final SnowflakeDialect dialect = new SnowflakeDialect();
        return new SQLExpressionVisitor(dialect, InferenceContext.from(Immutable.map()), v -> DSL.field(DSL.name(v.toString("_"))));
    }

    @Test
    void testGeneralIn() {

        final Expression expression = Expression.parseAndBind(Context.init(), "x in y");
        final String sql = visitor().visit(expression).toString();
        assertEquals("(ARRAY_CONTAINS(\"x\"::VARIANT, \"y\"))", sql.replaceAll("[\r\n\t]|(\\s\\s+)", ""));
    }

    @Test
    void testConstantIn() {

        final Expression expression = Expression.parseAndBind(Context.init(), "x in [1, 2, 3]");

        final String sql = visitor().visit(expression).toString();
        assertEquals("\"x\" in (1, 2, 3)", sql.replaceAll("[\r\n\t]|(\\s\\s+)", ""));
    }
}
