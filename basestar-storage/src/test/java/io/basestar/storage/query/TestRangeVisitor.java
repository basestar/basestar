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

import com.google.common.collect.ImmutableMap;
import io.basestar.expression.Expression;
import io.basestar.expression.compare.Eq;
import io.basestar.expression.compare.Gt;
import io.basestar.expression.compare.Gte;
import io.basestar.expression.compare.Lte;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.constant.PathConstant;
import io.basestar.expression.logical.And;
import io.basestar.util.Path;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestRangeVisitor {

    @Test
    public void testRange() {

        assertEquals(ImmutableMap.of(
                Path.of("a"), Range.eq(1),
                Path.of("b"), Range.gtLte(1, 3)
        ), range(new And(
                new Eq(new PathConstant("a"), new Constant(1)),
                new Lte(new PathConstant("b"), new Constant(3)),
                new Gt(new PathConstant("b"), new Constant(1)),
                new Gt(new PathConstant("b"), new Constant(1)),
                new Gte(new PathConstant("b"), new Constant(1))
        )));

        assertEquals(ImmutableMap.of(
                Path.of("b"), Range.eq(2)
        ), range(new And(
                new Gte(new PathConstant("b"), new Constant(2)),
                new Lte(new PathConstant("b"), new Constant(2))
        )));

        assertEquals(ImmutableMap.of(
                Path.of("b"), Range.invalid()
        ), range(new And(
                new Gt(new PathConstant("b"), new Constant(2)),
                new Lte(new PathConstant("b"), new Constant(2))
        )));

        assertEquals(ImmutableMap.of(), range(new And()));
    }

    private Map<Path, Range<Object>> range(final Expression e) {

        return new RangeVisitor().visit(e);
    }
}
