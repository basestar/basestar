package io.basestar.expression.text;

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
import io.basestar.expression.Binary;
import io.basestar.expression.Context;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.function.In;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public interface Like extends Binary {

    int PRECEDENCE = In.PRECEDENCE + 1;

    boolean isCaseSensitive();

    @Override
    default Boolean evaluate(final Context context) {

        final Object lhs = getLhs().evaluate(context);
        final Object rhs = getRhs().evaluate(context);
        final Matcher matcher = Matcher.Dialect.DEFAULT.parse(Objects.toString(rhs));
        return matcher.match(Objects.toString(lhs), isCaseSensitive());
    }

    @Override
    default int precedence() {

        return PRECEDENCE;
    }

    @Override
    default  <T> T visit(final ExpressionVisitor<T> visitor) {

        return visitor.visitLike(this);
    }

    @Data
    class Matcher {

        private final List<Term> terms;

        public Matcher(final List<Term> terms) {

            this.terms = ImmutableList.copyOf(terms);
        }

        public boolean match(final String str, final boolean caseSensitive) {

            final Pattern pattern = toRegex(caseSensitive);
            return pattern.matcher(str).matches();
        }

        public Pattern toRegex(final boolean caseSensitive) {

            final int flags = caseSensitive ? 0 : Pattern.CASE_INSENSITIVE;
            return Pattern.compile(terms.stream().map(Term::toRegex).collect(Collectors.joining()), flags);
        }

        public String toString(final Dialect dialect) {

            return terms.stream().map(v -> v.toString(dialect)).collect(Collectors.joining());
        }

        public interface Term {

            String toRegex();

            String toString(Dialect dialect);

            @Data
            class Literal implements Term {

                private final String text;

                @Override
                public String toRegex() {

                    return Pattern.quote(text);
                }

                @Override
                public String toString(final Dialect dialect) {

                    final StringBuilder str = new StringBuilder();
                    for(final char c : text.toCharArray()) {
                        if(c == dialect.getOne() || c == dialect.getAny() || c == dialect.getEscape()) {
                            str.append(dialect.getEscape());
                        }
                        str.append(c);
                    }
                    return str.toString();
                }
            }

            @Data
            class One implements Term {

                public static One INSTANCE = new One();

                @Override
                public String toRegex() {

                    return ".";
                }

                @Override
                public String toString(final Dialect dialect) {

                    return Character.toString(dialect.getOne());
                }
            }

            @Data
            class Any implements Term {

                public static Any INSTANCE = new Any();

                @Override
                public String toRegex() {

                    return ".*?";
                }

                @Override
                public String toString(final Dialect dialect) {

                    return Character.toString(dialect.getAny());
                }
            }
        }

        @Data
        public static class Dialect {

            public static final Dialect LUCENE = new Dialect('*', '?', '\\');

            public static final Dialect SQL = new Dialect('%', '_', '\\');

            public static final Dialect DEFAULT = SQL;

            private final char any;

            private final char one;

            private final char escape;

            public Matcher parse(final String str) {

                final List<Term> terms = new ArrayList<>();
                boolean escaping = false;
                StringBuilder buffer = null;
                for(final char c : str.toCharArray()) {
                    if(escaping) {
                        if(buffer == null) {
                            buffer = new StringBuilder();
                        }
                        buffer.append(c);
                        escaping = false;
                    } else if(c == escape) {
                        escaping = true;
                    } else if(c == any) {
                        if(buffer != null) {
                            terms.add(new Term.Literal(buffer.toString()));
                            buffer = null;
                        }
                        terms.add(Term.Any.INSTANCE);
                    } else if(c == one) {
                        if(buffer != null) {
                            terms.add(new Term.Literal(buffer.toString()));
                            buffer = null;
                        }
                        terms.add(Term.One.INSTANCE);
                    } else {
                        if(buffer == null) {
                            buffer = new StringBuilder();
                        }
                        buffer.append(c);
                    }
                }
                if(buffer != null) {
                    terms.add(new Term.Literal(buffer.toString()));
                }
                return new Matcher(terms);
            }

            public String toString(final Matcher matcher) {

                return matcher.toString(this);
            }
        }
    }
}
