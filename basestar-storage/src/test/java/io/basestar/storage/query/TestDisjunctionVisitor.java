package io.basestar.storage.query;

/*-
 * #%L
 * basestar-storage
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

import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.compare.Eq;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.constant.NameConstant;
import io.basestar.expression.iterate.ContextIterator;
import io.basestar.expression.iterate.ForAny;
import io.basestar.expression.logical.And;
import io.basestar.expression.logical.Or;
import io.basestar.util.Name;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestDisjunctionVisitor {

    @Test
    void testDisjunction() {

        final Eq a = new Eq(new NameConstant(Name.of("a")), new Constant(1));
        final Eq b = new Eq(new NameConstant(Name.of("b")), new Constant(2));
        final Eq c = new Eq(new NameConstant(Name.of("c")), new Constant(3));
        final Eq d = new Eq(new NameConstant(Name.of("d")), new Constant(3));
        final Eq e = new Eq(new NameConstant(Name.of("e")), new Constant(4));
        final Eq f = new Eq(new NameConstant(Name.of("f")), new Constant(5));
        final Eq g = new Eq(new NameConstant(Name.of("g")), new Constant(6));

        assertEquals(ImmutableSet.of(a, b), disjunction(new Or(a, b)));

        assertEquals(ImmutableSet.of(new And(a, c), new And(b, c)),
                disjunction(new And(new Or(a, b), c)));

        assertEquals(ImmutableSet.of(), disjunction(new And(new And(new Or(), a), b)));

        assertEquals(ImmutableSet.of(new And(a, b)),
                disjunction(new And(new Or(new Or(), a), b)));

        assertEquals(ImmutableSet.of(
                new And(a, d),
                new And(b, d),
                new And(c, d),
                e,
                new And(f, g)),
                disjunction(new Or(new And(new Or(a, b, c), d), new Or(e, new And(f, g)))));
    }

    private Set<Expression> disjunction(final Expression e) {

        return new DisjunctionVisitor().visit(e);
    }

    @Test
    void testIn() {

        final Expression root = Expression.parse("x.a in [1, 2]");
        final Expression bound = root.bind(Context.init());
        final Set<Expression> result = disjunction(bound);

        assertEquals(ImmutableSet.of(
            new Eq(
                    new NameConstant(Name.of("x", "a")),
                    new Constant(1L)
            ),
            new Eq(
                    new NameConstant(Name.of("x", "a")),
                    new Constant(2L)
            )
        ), result);
    }

    @Test
    void testForAnyOr() {

        final Expression root = Expression.parse("x.a || x.b for any x of y").bind(Context.init());
        final Set<Expression> result = disjunction(root);

        assertEquals(ImmutableSet.of(
                new ForAny(
                        new NameConstant(Name.of("x", "a")),
                        new ContextIterator.OfValue("x", new NameConstant(Name.of("y")))),
                new ForAny(
                        new NameConstant(Name.of("x", "b")),
                        new ContextIterator.OfValue("x", new NameConstant(Name.of("y"))))
        ), result);

        // x.a || x.b for any x of y
        // x.a for any x of y || x.b for any x of y
    }

    @Test
    void testForAnyIn() {

        final Expression root = Expression.parse("x.a in [1, 2] for any x of y");
        final Expression bound = root.bind(Context.init());
        final Set<Expression> result = disjunction(bound);

        assertEquals(ImmutableSet.of(
                new ForAny(
                        new Eq(
                                new NameConstant(Name.of("x", "a")),
                                new Constant(1L)
                        ),
                        new ContextIterator.OfValue("x", new NameConstant(Name.of("y")))),
                new ForAny(
                        new Eq(
                                new NameConstant(Name.of("x", "a")),
                                new Constant(2L)
                        ),
                        new ContextIterator.OfValue("x", new NameConstant(Name.of("y"))))
        ), result);

        // x.a || x.b for any x of y
        // x.a for any x of y || x.b for any x of y
    }
}
