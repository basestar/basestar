package io.basestar.schema.from;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.Argument;
import io.basestar.schema.use.UseNumber;
import io.basestar.schema.use.UseString;
import io.basestar.util.Immutable;
import io.basestar.util.Pair;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

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

    @Test
    public void testGetReplacedSqlWithBindings() {

        final String input = "SELECT * FROM source WHERE ${a}=${a} AND ${b}=2";
        final FromSql sql = new FromSql(input, Immutable.list(), Immutable.map(), null);
        final List<Argument> arguments = ImmutableList.of(
                Argument.builder().setName("a").setType(UseString.DEFAULT).build(null),
                Argument.builder().setName("b").setType(UseNumber.DEFAULT).build(null)
        );
        final Map<String, Object> values = ImmutableMap.of(
                "a", "str",
                "b", 1
        );
        final Pair<String, List<Object>> result = sql.getReplacedSqlWithBindings(schema -> schema.getQualifiedName().toString(), arguments, values);
        assertEquals("SELECT * FROM source WHERE ?=? AND ?=2", result.getFirst());
        assertEquals(ImmutableList.of("str", "str", 1.0), result.getSecond());
    }
}