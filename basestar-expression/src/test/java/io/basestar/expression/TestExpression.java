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
import io.basestar.expression.call.LambdaCall;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.constant.NameConstant;
import io.basestar.expression.function.With;
import io.basestar.expression.iterate.ForAll;
import io.basestar.expression.iterate.ForAny;
import io.basestar.expression.iterate.Where;
import io.basestar.expression.methods.Methods;
import io.basestar.expression.parse.ExpressionCache;
import io.basestar.expression.type.Values;
import io.basestar.util.Name;
import io.basestar.util.Pair;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
public class TestExpression {

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
    }

    private void assertEqualsPromoting(final Object a, final Object b) {

        final Pair<Object> pair = Values.promote(a, b);
        assertEquals(pair.getFirst(), pair.getSecond());
    }

    @Test
    public void testNull() {

        check("null", null);
    }

    @Test
    public void testPrecedence() {

        check("1 || 2 in [2]", true);
        check("2 + 3 * 5", 17);
        check("true || false ? 1 - 2 + \"test\" : false", "-1test");
        check("null ?? null ?? true", true);
        //check("[v + v for v in [0, 1, 2, 3, 4] where v]", Arrays.asList(2, 4, 6, 8));
    }

    @Test
    public void testArithmetic() {

        check("-1 - -2", 1);
        check("1 + 3", 4);
        check("1 + 3.2", 4.2);
        check("1.2 + 3", 4.2);
        check("10 / 2", 5);
        check("5 * 100", 500);
        check("7 % 3", 1);
        check("5 ** 2", 25);
        check("5.1 ** 4", 676.5200999999998);
    }

    @Test
    public void testLogical() {

        check("!\"\"", true);
        check("!\"a\"", false);
        check("!!!false", true);
        check("false && true", false);
        check("false || true", true);
    }

    @Test
    public void testLiteral() {

        check("{\"a\": 1, \"b\": [2, 3]}", ImmutableMap.of("a", 1L, "b", ImmutableList.of(2L, 3L)));
    }

    @Test
    public void testMember() {

        check("a.x.y", 5, context(ImmutableMap.of("a", ImmutableMap.of("x", ImmutableMap.of("y", 5)))));
    }

    @Test
    public void testIndex() {

        check("a[5]", 5, context(ImmutableMap.of("a", ImmutableList.of(0, 1, 2, 3, 4, 5))));
        check("\"test\"[2]", "s", context(ImmutableMap.of("a", ImmutableMap.of("x", 5))));
    }

    @Test
    public void testString() {

        check("'blah\\'blah'", "blah'blah");
        check("\"blah\\\"blah\"", "blah\"blah");
    }

    @Test
    public void testCompare() {

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
    public void testEquals() {

        Stream.of(
                Pair.of(ImmutableMap.of("a", 1), ImmutableMap.of("a", 1L)),
                Pair.of(ImmutableList.of("a", 1, 0L), ImmutableList.of("a", 1L, 0)),
                Pair.of(ImmutableSet.of(1L), ImmutableSet.of(1))
        ).forEach(pair -> {

            final Map<String, Object> scope = new HashMap<>();
            scope.put("a", pair.getFirst());
            scope.put("b", pair.getSecond());

            final Context context = context(scope);

            check("a == b", true, context);
        });
    }

    @Test
    public void testWith() {

        check("with(z = 100) with(x = 5, y = 10) (x + y) * z", 1500);
        //check("with(100 as z) with(5 as x, 10 as y) (x + y) * z", 1500);
    }

    @Test
    public void testLambda() {

        check("(x -> x + x)(1)", 2);
        check("((x, y) -> x * y)(2, 4)", 8);
        check("((x, y) -> z -> z * x + y)(2, 4)(6)", 16);
    }

    @Test
    public void testCall() {

        check("\"test\".size()", 4);
        check("[].size()", 0);
    }

    @Test
    public void testForObject() {

        check("{k : k for (k, v) of {\"a\":1, \"b\":2}}", ImmutableMap.of(
                "a", "a", "b", "b"
        ));
    }

    @Test
    public void testForArray() {

        check("[v.id for v of [{\"id\": 1}]]", ImmutableList.of(1L));
    }

    @Test
    public void testForSet() {

        check("{v.id for v of [{\"id\": \"a\"}, {\"id\": \"b\"}]}", ImmutableSet.of("a", "b"));
    }

    @Test
    public void testForAll() {

        check("v.id < 3 for all v of [{\"id\": 1}, {\"id\": 2}]", true);
        check("v.id < 3 for all v of [{\"id\": 2}, {\"id\": 3}]", false);
    }

    @Test
    public void testForAny() {

        check("v.id < 2 for any v of [{\"id\": 1}, {\"id\": 2}]", true);
        check("v.id < 2 for any v of [{\"id\": 2}, {\"id\": 3}]", false);
    }

    @Test
    public void testIn() {

        check("3 in [1, 2, 3, 4]", true);
        check("5 in [1, 2, 3, 4]", false);
    }

    @Test
    public void testWhere() {

        check("[v.id for v of [{\"id\": \"a\"}, {\"id\": \"b\"}] where v.id==\"a\"]", ImmutableList.of("a"));
    }

    @Test
    public void testComplex() {

        final String expr = "this.owner.id == caller.id && this.users.anyMatch(u -> u.id == caller.id)";
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
    public void testLambdaBind() {

        final Expression unbound = Expression.parse("[1].map(v -> a)").bind(context());
        assertTrue(unbound instanceof LambdaCall);
        final Expression bound = Expression.parse("[1].map(v -> v)").bind(context());
        assertTrue(bound instanceof Constant);
        final Expression chained = Expression.parse("x.map(v -> v.y.map(v2 -> v2))").bind(context(ImmutableMap.of(
                "x", ImmutableList.of(
                        ImmutableMap.of(
                                "y", ImmutableList.of(1)
                        )
                )
        )));
        assertTrue(chained instanceof Constant);

        final String str = "this.owner.id == caller.id || this.id in caller.projects.map(p -> p.id) || this.id in caller.teams.flatMap(t -> t.projects.map(p -> p.id))";
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
    public void testBitwise() {

        check("15315 << 2", 61260);
        check("15315 >> 5", 478);
        check("~15315", -15316);
        check("15315 & 13535", 12499);
        check("15315 | 13535", 16351);
        check("15315 ^ 13535", 3852);
    }

    // FIXME
    @Test
    @Disabled
    public void testStar() {

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
    public void testBindWith() {

        final Expression expression = Expression.parse("with(m = a) m");
        final Expression bound = expression.bind(Context.init(), NameTransform.root(Name.of("this")));
        assertEquals(Name.of("m"), ((NameConstant)((With)bound).getYield()).getName());
    }

    @Test
    public void testBindForAny() {

        final Expression expression = Expression.parse("m.id for any m of members");
        final Expression bound = expression.bind(Context.init(), NameTransform.root(Name.of("this")));
        assertEquals(Name.of("m", "id"), ((NameConstant)((ForAny)bound).getLhs()).getName());
    }

    @Test
    public void testBindForAll() {

        final Expression expression = Expression.parse("m.id for all m of members");
        final Expression bound = expression.bind(Context.init(), NameTransform.root(Name.of("this")));
        assertEquals(Name.of("m", "id"), ((NameConstant)((ForAll)bound).getLhs()).getName());
    }

    @Test
    public void testBindWhere() {

        final Expression expression = Expression.parse("m of members where m.id");
        final Expression bound = expression.bind(Context.init(), NameTransform.root(Name.of("this")));
        assertEquals(Name.of("m", "id"), ((NameConstant)((Where)bound).getRhs()).getName());
    }

    @Test
    public void testBindOf() {

        final Expression expression = Expression.parse("k of [1, 2, 3]");
        final Expression bound = expression.bind(Context.init());
        assertTrue(bound instanceof Constant);
    }

    @Test
    public void testBindForArray() {

        final Expression expression = Expression.parse("[k for k of [1, 2, 3]]");
        final Expression bound = expression.bind(Context.init());
        assertTrue(bound instanceof Constant);
    }

    @Test
    public void testBindForSet() {

        final Expression expression = Expression.parse("{k for k of [1, 2, 3]}");
        final Expression bound = expression.bind(Context.init());
        assertTrue(bound instanceof Constant);
    }

    @Test
    public void testBindForMap() {

        final Expression expression = Expression.parse("{k:k * 2 for k of [1, 2, 3]}");
        final Expression bound = expression.bind(Context.init());
        assertTrue(bound instanceof Constant);
    }

    @Test
    public void testLike() {

        final Expression expression = Expression.parse("'abc' like 'a%'");
        final Expression bound = expression.bind(Context.init());
    }

//    @Test
//    public void testPatternMatching() {
////
////        final Eq src = new Eq(new PathConstant(Path.of("hello")), new PathConstant(Path.of("world")));
////
////        final Path result = Eq.match(PathConstant.match(), PathConstant.match(), (p1, p2) -> p1.getPath().with(p2.getPath()))
////                .match(src);
////
////        assertEquals(Path.of("hello", "world"), result);
////
////        ForAny.match(null, Of.match(PathConstant.match(), p -> p.getPath()));
//
////        Expression.parse("x for any y of z");
//    }

    /*

        final Expression lhs = expression.getLhs();
        final Expression rhs = expression.getRhs();
        if(rhs instanceof Of) {
            final Of of = (Of)rhs;
            if(of.getExpr() instanceof PathConstant) {
                final Path path = ((PathConstant) of.getExpr()).getPath();
                // map keys not supported
                if(of.getKey() == null) {
                    final String value = of.getValue();
                    final Set<Path> paths = lhs.paths();

                    final Expression bound = lhs.bind(Context.init(), PathTransform.move(Path.of(value), path));


                    return null;
                }
            }
        }
        return null;
    }
     */
}
