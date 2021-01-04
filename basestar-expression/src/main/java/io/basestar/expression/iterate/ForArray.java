package io.basestar.expression.iterate;

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
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Array Comprehension
 *
 * Create an array from an iterator
 *
 */

@Data
public class ForArray implements For {

    public static final String TOKEN = "for";

    public static final int PRECEDENCE = ForSet.PRECEDENCE + 1;

    private final Expression yield;

    private final ContextIterator iterator;

    /**
     * [yield for iterator]
     *
     * @param yield expression Value-yielding expression
     * @param iterator iterator Iterator
     */

    public ForArray(final Expression yield, final ContextIterator iterator) {

        this.yield = yield;
        this.iterator = iterator;
    }

    @Override
    public List<Expression> getYields() {

        return ImmutableList.of(yield);
    }

    @Override
    public Expression create(final List<Expression> yields, final ContextIterator iterator) {

        assert yields.size() == 1;
        return new ForArray(yields.get(0), iterator);
    }

    @Override
    public List<?> evaluate(final Context context) {

        return evaluate(iterator.evaluate(context));
    }

    private List<?> evaluate(final Stream<Context> iter) {

        final List<Object> result = new ArrayList<>();
        iter.forEach(v -> {
            final Object value = yield.evaluate(v);
            result.add(value);
        });
        return result;
    }

    @Override
    public String token() {

        return TOKEN;
    }

    @Override
    public int precedence() {

        return PRECEDENCE;
    }

    @Override
    public <T> T visit(final ExpressionVisitor<T> visitor) {

        return visitor.visitForArray(this);
    }

    @Override
    public String toString() {

        return "[" + yield + " " + TOKEN + " " + iterator + "]";
    }
}
