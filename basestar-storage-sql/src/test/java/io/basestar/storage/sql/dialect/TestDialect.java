package io.basestar.storage.sql.dialect;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.basestar.schema.Argument;
import io.basestar.schema.from.FromSql;
import io.basestar.schema.use.UseNumber;
import io.basestar.schema.use.UseString;
import io.basestar.storage.sql.SQLDialect;
import io.basestar.util.Immutable;
import org.jooq.QueryPart;
import org.jooq.SQL;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.ReflectionUtils;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class TestDialect {

    protected abstract SQLDialect dialect();

    @Test
    protected void testGetReplacedSqlWithBindings() {

        final String input = "SELECT * FROM source WHERE ${a}=${a} AND ${b}=2";
        final FromSql sql = new FromSql(input, Immutable.list(), Immutable.map(), null);
        final List<Argument> arguments = ImmutableList.of(
                Argument.builder().setName("a").setType(UseString.DEFAULT).build(null),
                Argument.builder().setName("b").setType(UseNumber.DEFAULT).build(null)
        );
        final Map<String, Object> values = ImmutableMap.of(
                "a", "str",
                "b", 1.0
        );
        final SQL result = dialect().getReplacedSqlWithBindings(input, arguments, values);
        assertEquals("(SELECT * FROM source WHERE ?=? AND ?=2)", result.toString());
        // Using internal API of org.jooq.impl.SQLImpl
        @SuppressWarnings("unchecked") final List<QueryPart> substitutes = (List<QueryPart>) ReflectionUtils.tryToReadFieldValue((Class<SQL>) result.getClass(), "substitutes", result)
                .getOrThrow(IllegalStateException::new);
        assertEquals(ImmutableList.of(DSL.val("str"), DSL.val("str"), DSL.val(1.0)), substitutes);
    }
}
