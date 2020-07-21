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

import com.google.common.collect.ImmutableSet;
import io.basestar.expression.*;
import io.basestar.expression.type.Values;
import io.basestar.util.Name;
import io.basestar.util.Streams;
import lombok.Data;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * For All
 *
 * Universal Quantification: Returns true if the provided predicate is true for all iterator results
 *
 * @see io.basestar.expression.iterate.Of
 */

@Data
public class ForAll implements Binary {

    public static final String TOKEN = "for all";

    public static final int PRECEDENCE = ForAny.PRECEDENCE + 1;

    private final Expression lhs;

    private final Expression rhs;

    /**
     * lhs for all rhs
     *
     * @param lhs expression Predicate
     * @param rhs iterator Iterator
     */

    public ForAll(final Expression lhs, final Expression rhs) {

        this.lhs = lhs;
        this.rhs = rhs;
    }

    @Override
    public Expression create(final Expression lhs, final Expression rhs) {

        return new ForAll(lhs, rhs);
    }

//    @Override
//    public Expression bind(final Context context, final Path root) {
//
//        final Expression yield = this.yield.bind(context, root);
//        final Expression with = this.with.bind(context, root);
//        if(yield == this.yield && with == this.with) {
//            return this;
//        } else {
//            return new ForAll(yield, with);
//        }
//    }

    @Override
    public Boolean evaluate(final Context context) {

        final Object with = this.rhs.evaluate(context);
        if(with instanceof Iterator<?>) {
            return Streams.stream((Iterator<?>) with)
                    .allMatch(v -> {
                        @SuppressWarnings("unchecked")
                        final Map<String, Object> scope = (Map<String, Object>)v;
                        final Object value = this.lhs.evaluate(context.with(scope));
                        return Values.isTruthy(value);
                    });
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public Expression bindLhs(final Context context, final Renaming root) {

        return getLhs().bind(context, Renaming.closure(getRhs().closure(), root));
    }

    @Override
    public Set<Name> paths() {

        return ImmutableSet.<Name>builder()
                .addAll(lhs.paths())
                .addAll(rhs.paths())
                .build();
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

        return visitor.visitForAll(this);
    }

    @Override
    public String toString() {

        return lhs + " " + TOKEN + " " + rhs;
    }
}
