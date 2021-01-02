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
import io.basestar.expression.function.IfElse;
import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Object Comprehension
 *
 * Create an object from an iterator
 *
 */

@Data
public class ForObject implements For {

    public static final String TOKEN = "for";

    public static final int PRECEDENCE = IfElse.PRECEDENCE + 1;

    private final Expression yieldKey;

    private final Expression yieldValue;

    private final ContextIterator iterator;

    /**
     * {yieldKey:yieldValue for iterator}
     *
     * @param yieldKey expression Key-yielding expression
     * @param yieldValue expression Value-yielding expression
     * @param iterator iterator Iterator
     */

    public ForObject(final Expression yieldKey, final Expression yieldValue, final ContextIterator iterator) {

        this.yieldKey = yieldKey;
        this.yieldValue = yieldValue;
        this.iterator = iterator;
    }

    @Override
    public List<Expression> getYields() {

        return ImmutableList.of(yieldKey, yieldValue);
    }

    @Override
    public Expression create(final List<Expression> yields, final ContextIterator iterator) {

        assert yields.size() == 2;
        return new ForObject(yields.get(0), yields.get(1), iterator);
    }

    @Override
    public Map<String, ?> evaluate(final Context context) {

        return evaluate(iterator.evaluate(context));
    }

    private Map<String, ?> evaluate(final Stream<Context> iter) {

        final Map<String, Object> result = new HashMap<>();
        iter.forEach(v -> {
                    final Object key = yieldKey.evaluate(v);
                    final Object value = yieldValue.evaluate(v);
                    result.put(key.toString(), value);
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

        return visitor.visitForObject(this);
    }

    @Override
    public String toString() {

        return "{" + yieldKey + ": " + yieldValue + " " + TOKEN + " " + iterator + "}";
    }
}
