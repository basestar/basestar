//package io.basestar.expression.iterate;
//
///*-
// * #%L
// * basestar-expression
// * %%
// * Copyright (C) 2019 - 2020 Basestar.IO
// * %%
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *      http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// * #L%
// */
//
//import io.basestar.expression.*;
//import io.basestar.util.Streams;
//import lombok.Data;
//
//import java.util.Iterator;
//import java.util.Map;
//
///**
// * Where
// *
// * Filter an iterator using a predicate
// *
// * @see io.basestar.expression.iterate.Of
// */
//
//@Data
//public class Where implements Binary {
//
//    public static final String TOKEN = "where";
//
//    public static final int PRECEDENCE = Of.PRECEDENCE + 1;
//
//    private final Expression lhs;
//
//    private final Expression rhs;
//
//    /**
//     * lhs where rhs
//     *
//     * @param lhs iterator Iterator
//     * @param rhs predicate Expression to be evaluated for each iterator
//     */
//
//    public Where(final Expression lhs, final Expression rhs) {
//
//        this.lhs = lhs;
//        this.rhs = rhs;
//    }
//
//    @Override
//    public Expression create(final Expression lhs, final Expression rhs) {
//
//        return new Where(lhs, rhs);
//    }
//
//    @Override
//    public Expression bindRhs(final Context context, final Renaming root) {
//
//        return getRhs().bind(context, Renaming.closure(getLhs().closure(), root));
//    }
//
//    @Override
//    public Iterator<?> evaluate(final Context context) {
//
//        final Object lhs = this.lhs.evaluate(context);
//        if(lhs instanceof Iterator<?>) {
//            return Streams.stream((Iterator<?>)lhs)
//                    .filter(v -> {
//                        @SuppressWarnings("unchecked")
//                        final Map<String, Object> values = (Map<String, Object>)v;
//                        return rhs.evaluatePredicate(context.with(values));
//                    })
//                    .iterator();
//        } else {
//            throw new IllegalStateException();
//        }
//    }
//
//    @Override
//    public String token() {
//
//        return TOKEN;
//    }
//
//    @Override
//    public int precedence() {
//
//        return PRECEDENCE;
//    }
//
//    @Override
//    public <T> T visit(final ExpressionVisitor<T> visitor) {
//
//        return visitor.visitWhere(this);
//    }
//
//    @Override
//    public String toString() {
//
//        return lhs + " " + TOKEN + " " + rhs;
//    }
//}
