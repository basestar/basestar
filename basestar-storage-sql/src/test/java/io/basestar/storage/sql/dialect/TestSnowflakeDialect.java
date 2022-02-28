package io.basestar.storage.sql.dialect;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.Argument;
import io.basestar.schema.Property;
import io.basestar.schema.StructSchema;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.from.FromSql;
import io.basestar.schema.use.UseInteger;
import io.basestar.schema.use.UseStruct;
import io.basestar.storage.sql.SQLDialect;
import io.basestar.storage.sql.SQLExpressionVisitor;
import io.basestar.util.Immutable;
import org.jooq.QueryPart;
import org.jooq.SQL;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.ReflectionUtils;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestSnowflakeDialect extends TestDialect {

    private SQLExpressionVisitor visitor() {

        final SQLDialect dialect = dialect();
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

    @Override
    protected SQLDialect dialect() {

        return new SnowflakeDialect();
    }

    @Test
    protected void testGetReplacedSqlWithStructBindings() {

        final String input = "SELECT * FROM source WHERE x=${point}['x'] AND y=${point}['y']";
        final FromSql sql = new FromSql(input, Immutable.list(), Immutable.map(), null);
        final List<Argument> arguments = ImmutableList.of(
                Argument.builder().setName("point").setType(new UseStruct(StructSchema.builder()
                        .setProperty("x", Property.builder().setType(UseInteger.DEFAULT))
                        .setProperty("y", Property.builder().setType(UseInteger.DEFAULT))
                        .build())).build(null)
        );
        final Map<String, Object> values = ImmutableMap.of(
                "point", Immutable.map(
                        "x", 1,
                        "y", 2
                )
        );
        final SQL result = dialect().getReplacedSqlWithBindings(input, arguments, values);
        assertEquals("(SELECT * FROM source WHERE x=?['x'] AND y=?['y'])", result.toString());
        // Using internal API of org.jooq.impl.SQLImpl
        @SuppressWarnings("unchecked") final List<QueryPart> substitutes = (List<QueryPart>) ReflectionUtils.tryToReadFieldValue((Class<SQL>) result.getClass(), "substitutes", result)
                .getOrThrow(IllegalStateException::new);
        final SQL point = DSL.sql("parse_json('{\"x\":1,\"y\":2}')");
        assertEquals(ImmutableList.of(point, point), substitutes);
    }
}
