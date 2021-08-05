package io.basestar.schema.from;

import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.util.Pair;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestFromSql {

    @Test
    public void testOldStyleJoin() {

        final Expression.Closure closure = Expression.Closure.from(ImmutableSet.of("a", "b", "c"));
        final JoinExpressionVisitor visitor = new JoinExpressionVisitor(closure);

        assertEquals(Pair.of(
                Expression.parseAndBind(Context.init(), "a.x == b.x && (c.x == b.x || c.y == b.y)"),
                Expression.parseAndBind(Context.init(), "c.x == d.y")
        ), visitor.visitAndSimplify(Expression.parse("a.x == b.x && (c.x == b.x || c.y == b.y) && c.x == d.y")));

        assertEquals(Pair.of(
                Expression.parseAndBind(Context.init(), "a.x == b.x && c.x == b.y"),
                Expression.parseAndBind(Context.init(), "a.x == d.y")
        ), visitor.visitAndSimplify(Expression.parse("a.x == b.x && a.x == d.y && c.x == b.y")));
    }
}
