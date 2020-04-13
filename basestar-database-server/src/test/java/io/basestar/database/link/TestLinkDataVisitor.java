package io.basestar.database.link;

import com.google.common.collect.ImmutableMap;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestLinkDataVisitor {

    @Test
    public void testSimple() {

        final Expression expression = Expression.parse("team.id == this.id");
        final Expression bound = expression.bind(Context.init(ImmutableMap.of(
                "this", ImmutableMap.of("id", "x")
        )));
        final Map<String, Object> linkData = bound.visit(new LinkDataVisitor());
        assertEquals(ImmutableMap.of("team", ImmutableMap.of("id", "x")), linkData);
    }

    @Test
    public void testComplex() {

        final Expression expression = Expression.parse("team.id == this.id && team.role.id == this.role.id");
        final Expression bound = expression.bind(Context.init(ImmutableMap.of(
                "this", ImmutableMap.of(
                        "id", "x",
                        "role", ImmutableMap.of("id", "y"))
        )));
        final Map<String, Object> linkData = bound.visit(new LinkDataVisitor());
        assertEquals(ImmutableMap.of("team", ImmutableMap.of(
                "id", "x",
                "role", ImmutableMap.of("id", "y")
        )), linkData);
    }

    @Test
    public void testImpossibleAnd() {

        final Expression expression = Expression.parse("team.id == this.id && team.id == this.id2");
        final Expression bound = expression.bind(Context.init(ImmutableMap.of(
                "this", ImmutableMap.of("id", "x", "id2", "y")
        )));
        final Map<String, Object> linkData = bound.visit(new LinkDataVisitor());
        assertEquals(ImmutableMap.of("team", ImmutableMap.of()), linkData);
    }

    @Test
    public void testImpossibleOr() {

        final Expression expression = Expression.parse("team.id == this.id || team.role == this.role");
        final Expression bound = expression.bind(Context.init(ImmutableMap.of(
                "this", ImmutableMap.of("id", "x", "role", "y")
        )));
        final Map<String, Object> linkData = bound.visit(new LinkDataVisitor());
        assertEquals(ImmutableMap.of(), linkData);
    }
}
