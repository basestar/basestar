package io.basestar.storage.query;

import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestAggregatesVisitor {

    @Test
    public void testAggregates() {

        final Expression expr = Expression.parse("sum(y) + round(sum(x))").bind(Context.init());

        final AggregatesVisitor visitor = new AggregatesVisitor();
        final Expression transformed = visitor.visit(expr);

        assertEquals(2, visitor.getAggregates().size());
    }
}
