package io.basestar.expression.function;

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

import io.basestar.expression.Binary;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import lombok.Data;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Index
 *
 */

@Data
public class Index implements Binary {

    public static final String TOKEN = "[]";

    public static final int PRECEDENCE = Member.PRECEDENCE + 1;

    private final Expression lhs;

    private final Expression rhs;

    /**
     * lhs[rhs]
     *
     * @param lhs collection|object Left hand operand
     * @param rhs any Right hand operand
     */

    public Index(final Expression lhs, final Expression rhs) {

        this.lhs = lhs;
        this.rhs = rhs;
    }

    @Override
    public Expression create(final Expression lhs, final Expression rhs) {

        return new Index(lhs, rhs);
    }

    @Override
    public Object evaluate(final Context context) {

        final Object lhs = this.lhs.evaluate(context);
        final Object rhs = this.rhs.evaluate(context);
        if(lhs instanceof List<?> && rhs instanceof Number) {
            // FIXME float index should probably throw
            return ((List<?>)lhs).get(((Number)rhs).intValue());
        } else if(lhs instanceof Set<?>) {
            return ((Set<?>)lhs).contains(rhs);
        } else if(lhs instanceof Map<?, ?>) {
            return ((Map<?, ?>)lhs).get(rhs);
        } else if(lhs instanceof String && rhs instanceof Number) {
            final String str = ((String)lhs);
            final int at = ((Number)rhs).intValue();
            return Character.toString(str.charAt(at));
        } else {
            throw new IllegalStateException();
        }
    }

//    @Override
//    public Query query() {
//
//        return Query.and();
//    }

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

        return visitor.visitIndex(this);
    }

    @Override
    public String toString() {

        return lhs + "[" + rhs + "]";
    }
}
