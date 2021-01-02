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

import java.util.List;

/**
 * For Any
 *
 * Existential Quantification: Returns true if the provided predicate is true for any iterator result
 *
 */

@Data
public class ForAny implements For {

    public static final String TOKEN = "for any";

    public static final int PRECEDENCE = ForArray.PRECEDENCE + 1;

    private final Expression yield;

    private final ContextIterator iterator;

    /**
     * lhs for any iterator
     *
     * @param yield expression Predicate
     * @param iterator iterator Iterator
     */

    public ForAny(final Expression yield, final ContextIterator iterator) {

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
        return new ForAny(yields.get(0), iterator);
    }

    @Override
    public Boolean evaluate(final Context context) {

        return iterator.evaluate(context).anyMatch(yield::evaluatePredicate);
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

        return visitor.visitForAny(this);
    }

    @Override
    public String toString() {

        return yield + " " + TOKEN + " " + iterator;
    }
}
