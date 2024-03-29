package io.basestar.expression;

/*-
 * #%L
 * basestar-expression
 * %%
 * Copyright (C) 2019 - 2020 Basestar.IO
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.expression.aggregate.Aggregate;
import io.basestar.expression.call.LambdaCall;
import io.basestar.expression.compare.Eq;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.constant.NameConstant;
import io.basestar.expression.exception.BadExpressionException;
import io.basestar.expression.exception.BadOperandsException;
import io.basestar.expression.function.With;
import io.basestar.expression.iterate.ForAll;
import io.basestar.expression.iterate.ForAny;
import io.basestar.expression.methods.Methods;
import io.basestar.expression.parse.ExpressionCache;
import io.basestar.expression.sql.From;
import io.basestar.expression.sql.Select;
import io.basestar.expression.sql.Sql;
import io.basestar.expression.sql.Union;
import io.basestar.expression.type.Values;
import io.basestar.util.Name;
import io.basestar.util.Pair;
import io.basestar.util.Sort;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
class TestExpression {

    private final ExpressionCache cache = new ExpressionCache();

    private final Methods methods = Methods.builder()
            .defaults()
            .build();

    private Context context() {

        return context(ImmutableMap.of());
    }

    private Context context(final Map<String, Object> scope) {

        return Context.init(methods, scope);
    }

    private void check(final String expr, final Object expected) {

        check(expr, expected, context(Collections.emptyMap()));
    }

    private void check(final String expr, final Object expected, final Context context) {

        final Expression expression = cache.parse(expr);
        final Object actual = expression.evaluate(context);

        assertEqualsPromoting(expected, actual);

        final String string = expression.toString();

        log.debug("Expression ({}) reparse as: ({})", expr, string);

        final Expression reparsed = cache.parse(string);
        final Object actualReparsed = reparsed.evaluate(context);
        assertEqualsPromoting(expected, actualReparsed);

        final Expression optimized = expression.bind(context(Collections.emptyMap()));
        final Object actualOptimized = optimized.evaluate(context);

        assertEqualsPromoting(expected, actualOptimized);

        final Expression bound = expression.bind(context);
        final Object actualBound = bound.evaluate(context);

        assertEqualsPromoting(expected, actualBound);

        final Expression copy = expression.visit(new ExpressionVisitor.Defaulting<Expression>() {
            @Override
            public Expression visitDefault(final Expression expression) {

                return expression.copy(this::visit);
            }
        });
        assertEquals(expression, copy);

        final Set<Name> names = expression.names();
        assertTrue(expression.isConstant(Name.branch(names).keySet()));
    }

    private void checkAggregate(final String expr, final Object expected, final Supplier<Stream<Context>> context) {

        final Aggregate expression = (Aggregate) cache.parse(expr);
        final Object actual = expression.evaluate(context.get());

        assertEqualsPromoting(expected, actual);

        final String string = expression.toString();

        log.debug("Expression ({}) reparse as: ({})", expr, string);

        final Aggregate reparsed = (Aggregate) cache.parse(string);
        final Object actualReparsed = reparsed.evaluate(context.get());
        assertEqualsPromoting(expected, actualReparsed);

        final Expression copy = expression.visit(new ExpressionVisitor.Defaulting<Expression>() {
            @Override
            public Expression visitDefault(final Expression expression) {

                return expression.copy(this::visit);
            }
        });
        assertEquals(expression, copy);
    }

    private void assertEqualsPromoting(final Object a, final Object b) {

        final Pair<Object, Object> pair = Values.promote(a, b);
        assertEquals(pair.getFirst(), pair.getSecond());
    }

    private void checkBadOperands(final String expr) {

        final Context context = Context.init();
        final Expression expression = cache.parse(expr);
        assertThrows(BadOperandsException.class, () -> expression.evaluate(context));
    }

    @Test
    void testNull() {

        check("null", null);
    }

    @Test
    void testPrecedence() {

        check("1 || 2 in [2]", true);
        check("2 + 3 * 6", 20);
        check("true || false ? 1 - 2 + \"test\" : false", "-1test");
        check("null ?? null ?? true", true);
    }

    @Test
    void testAdd() {

        check("1 + 3", 4);
        check("1 + 3.2", 4.2);
        check("1.2 + 3", 4.2);
        check("'x' + 1", "x1");
        check("[1] + [2]", ImmutableList.of(1L, 2L));
        check("{1} + {5}", ImmutableSet.of(1L, 5L));
        check("{'x': 1} + {'y': 2}", ImmutableMap.of("x", 1L, "y", 2L));
        checkBadOperands("1 + []");
        checkBadOperands("{'x': 1} + []");
    }

    @Test
    void testSub() {

        check("-1 - -2", 1);
        check("2.5 - 1", 1.5);
        check("2.5 - 1.5", 1.0);
        checkBadOperands("1 - []");
    }

    @Test
    void testDiv() {

        check("10 / 2", 5);
        check("15 / 2", 7);
        check("15.0 / 2", 7.5);
        checkBadOperands("1 / []");
    }

    @Test
    void testMul() {

        check("5 * 100", 500);
        check("4 * 0.1", 0.4);
        checkBadOperands("1 * []");
    }

    @Test
    void testMod() {

        check("7 % 3", 1);
        checkBadOperands("1.5 % 2.5");
        checkBadOperands("1 % []");
    }

    @Test
    void testPow() {

        check("5 ** 2", 25);
        check("5.1 ** 4", 676.5200999999998);
        checkBadOperands("1 ** []");
    }

    @Test
    void testNegate() {

        check("-5", -5);
        check("-5.5", -5.5);
        checkBadOperands("-'x'");
    }

    @Test
    void testLogical() {

        check("!\"\"", true);
        check("!\"a\"", false);
        check("!!!false", true);
        check("false && true", false);
        check("false || true", true);
        check("not \"\"", true);
        check("NOT \"a\"", false);
        check("not NOT not false", true);
        check("false and true", false);
        check("false OR true", true);
    }

    @Test
    void testLiteral() {

        check("{\"a\": 1, \"b\": [2, 3]}", ImmutableMap.of("a", 1L, "b", ImmutableList.of(2L, 3L)));
    }

    @Test
    void testMember() {

        check("a.x.y", 5, context(ImmutableMap.of("a", ImmutableMap.of("x", ImmutableMap.of("y", 5)))));
    }

    @Test
    void testIndex() {

        check("a[5]", 5, context(ImmutableMap.of("a", ImmutableList.of(0, 1, 2, 3, 4, 5))));
        check("\"test\"[2]", "s", context(ImmutableMap.of("a", ImmutableMap.of("x", 5))));
    }

    @Test
    void testString() {

        check("'blah\\'blah'", "blah'blah");
        check("\"blah\\\"blah\"", "blah\"blah");
    }

    @Test
    void testCompare() {

        Stream.of(
                Pair.of(1, 2),
                Pair.of("a", "b"),
                Pair.of(1.0, 2),
                Pair.of(false, true),
                Pair.of(false, 1)
        ).forEach(pair -> {

            final Map<String, Object> scope = new HashMap<>();
            scope.put("a", pair.getFirst());
            scope.put("b", pair.getSecond());

            final Context context = context(scope);

            check("a <=> a", 0, context);
            check("a <=> b", -1, context);
            check("b <=> a", 1, context);
            check("a == a", true, context);
            check("a == b", false, context);
            check("a != a", false, context);
            check("a != b", true, context);
            check("a < a", false, context);
            check("a < b", true, context);
            check("b < a", false, context);
            check("a <= a", true, context);
            check("a <= b", true, context);
            check("b <= a", false, context);
            check("a > a", false, context);
            check("a > b", false, context);
            check("b > a", true, context);
            check("a >= a", true, context);
            check("a >= b", false, context);
            check("b >= a", true, context);

        });
    }

    @Test
    void testEquals() {

        check("'a:b' == \"a:b\"", true, Context.init());

        Stream.of(
                Pair.of(ImmutableMap.of("a", 1), ImmutableMap.of("a", 1L)),
                Pair.of(ImmutableList.of("a", 1, 0L), ImmutableList.of("a", 1L, 0)),
                Pair.of(ImmutableSet.of(1L), ImmutableSet.of(1)),
                Pair.of(ImmutableSet.of("a:b"), ImmutableSet.of("a:b"))
        ).forEach(pair -> {

            final Map<String, Object> scope = new HashMap<>();
            scope.put("a", pair.getFirst());
            scope.put("b", pair.getSecond());

            final Context context = context(scope);

            check("a == b", true, context);
            check("a = b", true, context);
        });
    }

    @Test
    @Disabled
    void testWith() {

        check("with(z = 100) with(x = 5, y = 10) (x + y) * z", 1500);
        //check("with(100 as z) with(5 as x, 10 as y) (x + y) * z", 1500);
    }

    @Test
    void testLambda() {

        check("(x => x + x)(1)", 2);
        check("((x, y) => x * y)(2, 4)", 8);
        check("((x, y) => z => z * x + y)(2, 4)(6)", 16);
    }

    @Test
    void testCall() {

        check("\"test\".size()", 4);
        check("[].size()", 0);
    }

    @Test
    void testForObject() {

        check("{k : k for (k, v) of {\"a\":1, \"b\":2}}", ImmutableMap.of(
                "a", "a", "b", "b"
        ));
    }

    @Test
    void testForArray() {

        check("[v.id for v of [{\"id\": 1}]]", ImmutableList.of(1L));
    }

    @Test
    void testForSet() {

        check("{v.id for v of [{\"id\": \"a\"}, {\"id\": \"b\"}]}", ImmutableSet.of("a", "b"));
    }

    @Test
    void testForAll() {

        check("v.id < 3 for all v of [{\"id\": 1}, {\"id\": 2}]", true);
        check("v.id < 3 for all v of [{\"id\": 2}, {\"id\": 3}]", false);
    }

    @Test
    void testForAny() {

        check("v.id < 2 for any v of [{\"id\": 1}, {\"id\": 2}]", true);
        check("v.id < 2 for any v of [{\"id\": 2}, {\"id\": 3}]", false);
    }

    @Test
    void testIn() {

        check("3 In [1, 2, 3, 4]", true);
        check("3 in [1, 2, 3, 4]", true);
        check("5 in [1, 2, 3, 4]", false);
    }

    @Test
    void testWhere() {

        check("[v.id for v of [{\"id\": \"a\"}, {\"id\": \"b\"}] where v.id==\"a\"]", ImmutableList.of("a"));
    }

    @Test
    void testComplex() {

        final String expr = "this.owner.id == caller.id && this.users.anyMatch(u => u.id == caller.id)";
        check(expr, true, context(ImmutableMap.of(
                "caller", ImmutableMap.of(
                        "id", "test"
                ),
                "this", ImmutableMap.of(
                        "owner", ImmutableMap.of(
                                "id", "test"
                        ),
                        "users", ImmutableList.of(
                                ImmutableMap.of(
                                        "id", "test"
                                )
                        )
                )
        )));
    }

    @Test
    @Disabled
    void testLambdaBind() {

        final Expression unbound = Expression.parse("[1].map(v => a)").bind(context());
        assertTrue(unbound instanceof LambdaCall);
        final Expression bound = Expression.parse("[1].map(v => v)").bind(context());
        assertTrue(bound instanceof Constant);
        final Expression chained = Expression.parse("x.map(v => v.y.map(v2 => v2))").bind(context(ImmutableMap.of(
                "x", ImmutableList.of(
                        ImmutableMap.of(
                                "y", ImmutableList.of(1)
                        )
                )
        )));
        assertTrue(chained instanceof Constant);

        final String str = "this.owner.id == caller.id || this.id in caller.projects.map(p => p.id) || this.id in caller.teams.flatMap(t => t.projects.map(p => p.id))";
        final Expression example = Expression.parse(str).bind(context(ImmutableMap.of())).bind(context(ImmutableMap.of(
                "this", ImmutableMap.of(
                        "id", "a",
                        "owner", ImmutableMap.of(
                                "id", "test"
                        )
                ),
                "caller", ImmutableMap.of(
                        "id", "test",
                        "projects", ImmutableList.of(
                                ImmutableMap.of(
                                        "project", ImmutableMap.of(
                                                "id", "b"
                                        )
                                )
                        ),
                        "teams", ImmutableList.of(
                                ImmutableMap.of(
                                        "projects", ImmutableList.of(
                                                ImmutableMap.of(
                                                        "id", "c"
                                                )
                                        )
                                )
                        )
                )
        )));
        assertTrue(example instanceof Constant);
    }

    @Test
    void testBitwise() {

        check("15315 << 2", 61260);
        check("15315 >> 5", 478);
        check("~15315", -15316);
        check("15315 & 13535", 12499);
        check("15315 | 13535", 16351);
        check("15315 ^ 13535", 3852);
        check("15315 xor 13535", 3852);
    }

    @Test
    void testLike() {

        check("'abc' like 'a%'", true);
        check("'abc' like 'A%'", false);
        check("'abc' ilike 'A%'", true);
        check("'abc' like 'A_C'", false);
        check("'abc' ilike 'A_C'", true);
    }

    // FIXME
    @Test
    @Disabled
    void testStar() {

        check("a.*.x", Arrays.asList(1, 2), context(ImmutableMap.of(
                "a", ImmutableList.of(
                        ImmutableMap.of("x", 1),
                        ImmutableMap.of("x", 2)
                )
        )));

        check("a.*.x.sort()", Arrays.asList(1, 2), context(ImmutableMap.of(
                "a", ImmutableMap.of(
                        "a", ImmutableMap.of("x", 1),
                        "b", ImmutableMap.of("x", 2)
                )
        )));

        check("1 in a.*.x", true, context(ImmutableMap.of(
                "a", ImmutableList.of(
                        ImmutableMap.of("x", 1),
                        ImmutableMap.of("x", 2)
                )
        )));

        check("3 in a.*.x", false, context(ImmutableMap.of(
                "a", ImmutableList.of(
                        ImmutableMap.of("x", 1),
                        ImmutableMap.of("x", 2)
                )
        )));
    }

    @Test
    @Disabled
    void testBindWith() {

        final Expression expression = Expression.parse("with(m = a) m");
        final Expression bound = expression.bind(Context.init(), Renaming.addPrefix(Name.of("this")));
        assertEquals(Name.of("m"), ((NameConstant) ((With) bound).getYield()).getName());
    }

    @Test
    void testBindForAny() {

        final Expression expression = Expression.parse("m.id for any m of members");
        final Expression bound = expression.bind(Context.init(), Renaming.addPrefix(Name.of("this")));
        assertEquals(Name.of("m", "id"), ((NameConstant) ((ForAny) bound).getYield()).getName());
    }

    @Test
    void testBindForAll() {

        final Expression expression = Expression.parse("m.id for all m of members");
        final Expression bound = expression.bind(Context.init(), Renaming.addPrefix(Name.of("this")));
        assertEquals(Name.of("m", "id"), ((NameConstant) ((ForAll) bound).getYield()).getName());
    }

    @Test
    void testBindForArray() {

        final Expression expression = Expression.parse("[k for k of [1, 2, 3]]");
        final Expression bound = expression.bind(Context.init());
        assertTrue(bound instanceof Constant);
    }

    @Test
    void testBindForSet() {

        final Expression expression = Expression.parse("{k for k of [1, 2, 3]}");
        final Expression bound = expression.bind(Context.init());
        assertTrue(bound instanceof Constant);
    }

    @Test
    void testBindForMap() {

        final Expression expression = Expression.parse("{k:k * 2 for k of [1, 2, 3]}");
        final Expression bound = expression.bind(Context.init());
        assertTrue(bound instanceof Constant);
    }

    @Test
    @Disabled
    void testThrowOnSingleEquals() {

        assertThrows(BadExpressionException.class, () -> Expression.parse("x = y"));
    }

    @Test
    void testExtant() {

        Expression.parse("asset.fileType != 'UNKNOWN' && location.locationType == 'INBOX' && ('2018' IN asset.tags || 'student' IN asset.tags)");
        Expression.parse("asset.fileType != 'UNKNOWN' && location.locationType == 'INBOX' && ('DSR' IN asset.fileType || 'ERN' IN asset.fileType)");
        final Expression original = Expression.parse("(sdnRecord.accountingPeriodStart.toInteger() + ((sdnRecord.accountingPeriodEnd.toInteger() - sdnRecord.accountingPeriodStart.toInteger())/2)).toDate().toString(\"YYYY-'QQ\")");
        final Expression reparsed = Expression.parse(original.toString());
        assertEquals(original, reparsed);
    }

    @Test
    void testDateTimeFormat() {

        check("v.toString('YYYY')", "2020", context(ImmutableMap.of(
                "v", LocalDate.parse("2020-01-01")
        )));

        check("v.toString()", "2020-01-01T01:02:03.000Z", context(ImmutableMap.of(
                "v", Instant.parse("2020-01-01T01:02:03Z")
        )));

        check("v.toDate()", LocalDate.parse("2020-01-01"), context(ImmutableMap.of(
                "v", "2020-01-01"
        )));

        check("v.toDate('dd-MM-yyyy')", LocalDate.parse("2020-01-01"), context(ImmutableMap.of(
                "v", "01-01-2020"
        )));

        check("v.toDatetime('dd-MM-yyyy HH:mm:ss X')", Instant.parse("2020-01-01T01:02:03Z"), context(ImmutableMap.of(
                "v", "01-01-2020 01:02:03 Z"
        )));

        check("v.toDatetime('dd-MM-yyyy HH:mm:ss')", Instant.parse("2020-01-01T01:02:03Z"), context(ImmutableMap.of(
                "v", "01-01-2020 01:02:03"
        )));

        check("v.toDate().toInteger()", 1577836800000L, context(ImmutableMap.of(
                "v", "2020-01-01"
        )));

        check("null.toDatetime()", null, context(ImmutableMap.of()));
    }

    @Test
    void testAggregates() {

        final List<Long> numbers = ImmutableList.of(1L, 4L, 9L, 6L, 7L, 8L, 2L, 3L);

        final Supplier<Stream<Context>> stream = () -> numbers.stream().map(v -> Context.init(ImmutableMap.of("x", v)));

        checkAggregate("max(x)", 9, stream);
        checkAggregate("min(x)", 1, stream);
        checkAggregate("count()", 8, stream);
        checkAggregate("count(x > 5)", 4, stream);
        checkAggregate("sum(x)", 40, stream);
        checkAggregate("avg(x)", 5.0, stream);
        checkAggregate("collectArray(x)", numbers, stream);
    }

    @Test
    void testSql() {

        final Expression expr = cache.parse("WITH x AS (y), y AS (z) SELECT x, y yy, * FROM a LEFT OUTER JOIN c AS cc ON true WHERE a = b GROUP BY e ORDER BY d DESC UNION x UNION ALL y");

        final Sql select = new Sql(
                ImmutableList.of(
                        new Select.Anonymous(new NameConstant("x")),
                        new Select.Named(new NameConstant("y"), "yy"),
                        new Select.All()
                ),
                ImmutableList.of(
                        new From.Join(
                                new From.Anonymous(new NameConstant("a")),
                                new From.Named(new NameConstant("c"), "cc"),
                                new Constant(true),
                                From.Join.Type.LEFT_OUTER
                        )
                ),
                new Eq(
                        new NameConstant("a"),
                        new NameConstant("b")
                ),
                ImmutableList.of(
                        new NameConstant("e")
                ),
                ImmutableList.of(
                        Sort.desc(Name.of("d"))
                ),
                ImmutableList.of(
                        new Union.Distinct(new NameConstant("x")),
                        new Union.All(new NameConstant("y"))
                )
        );

        assertEquals(new With(
                ImmutableList.of(Pair.of("x", new NameConstant("y")), Pair.of("y", new NameConstant("z"))),
                select
        ), expr);
    }

    @Test
    void test() {

        Expression.parse("SELECT wer.fileId, \"wer_20\" AS type, wer.currency, wer.grossRoyaltyAmount, wer.taxAmount, wer.commissionAmount, wer.grossRoyaltyAmount - wer.taxAmount - wer.commissionAmount AS netRoyaltyAmount, 0 AS adjustmentAmount, 0 AS returnedAmount FROM wer20 AS wer UNION DISTINCT SELECT wep.fileId, \"wep_20\" AS type, wep.currency, wep.grossRoyaltyAmount, wep.taxAmount, wep.commissionAmount, wep.grossRoyaltyAmount - wep.taxAmount - wep.commissionAmount AS netRoyaltyAmount, 0 AS adjustmentAmount, 0 AS returnedAmount FROM wep20 AS wep UNION DISTINCT SELECT wea.fileId, \"wea_20\" AS type, wea.currency, wea.grossRoyaltyAmount, wea.taxAmount, wea.commissionAmount, wea.grossRoyaltyAmount - wea.taxAmount - wea.commissionAmount AS netRoyaltyAmount, 0 AS adjustmentAmount, 0 AS returnedAmount FROM wea20 AS wea UNION DISTINCT SELECT wbi.fileId, \"wbi_20\" AS type, wbi.currency, wbi.grossRoyaltyAmount, wbi.taxAmount, wbi.commissionAmount, wbi.grossRoyaltyAmount - wbi.taxAmount - wbi.commissionAmount AS netRoyaltyAmount, 0 AS adjustmentAmount, 0 AS returnedAmount FROM wbi20 AS wbi UNION DISTINCT SELECT icc.fileId, \"icc_20\" AS type, icc.currency, 0 AS grossRoyaltyAmount, 0 AS taxAmount, 0 AS commissionAmount, CASE icc.remittingSocietyContingencyAmountSign WHEN \"-\" THEN -icc.remittingSocietyContingencyAmount ELSE icc.remittingSocietyContingencyAmount END AS netRoyaltyAmount, 0 AS adjustmentAmount, 0 AS returnedAmount FROM icc20 AS icc UNION DISTINCT SELECT adj.fileId, \"adj_20\" AS type, sdn.remittanceCurrency AS currency, 0 AS grossRoyaltyAmount, 0 AS taxAmount, 0 AS commissionAmount, 0 AS netRoyaltyAmount, CASE adj.adjustmentAmountSign WHEN \"-\" THEN -adj.adjustmentAmount ELSE adj.adjustmentAmount END AS adjustmentAmount, 0 AS returnedAmount FROM adj20 AS adj, sdn20 AS sdn WHERE adj.fileId == sdn.fileId UNION DISTINCT SELECT rrp.fileId, \"rrp_20\" AS type, sdn.remittanceCurrency AS currency, 0 AS grossRoyaltyAmount, 0 AS taxAmount, 0 AS commissionAmount, 0 AS netRoyaltyAmount, 0 AS adjustmentAmount, CASE rrp.returnAmountSign WHEN \"-\" THEN -rrp.returnAmount ELSE rrp.returnAmount END AS returnedAmount FROM rrp20 AS rrp, sdn20 AS sdn WHERE rrp.fileId == sdn.fileId");
    }
}