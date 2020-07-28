package io.basestar.expression.parse;

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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.basestar.expression.Expression;
import io.basestar.expression.exception.BadExpressionException;
import io.basestar.util.Throwables;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

import javax.annotation.Nonnull;

public class ExpressionCache {

    private final LoadingCache<String, Expression> cache = CacheBuilder.newBuilder()
            .build(new CacheLoader<String, Expression>() {
                @Override
                public Expression load(@Nonnull final String expr) {

                    final ExpressionLexer lexer = new ExpressionLexer(CharStreams.fromString(expr));
                    lexer.removeErrorListeners();
                    lexer.addErrorListener(ThrowingErrorListener.INSTANCE);
                    final CommonTokenStream tokens = new CommonTokenStream(lexer);
                    final ExpressionParser parser = new ExpressionParser(tokens);
                    parser.removeErrorListeners();
                    parser.addErrorListener(ThrowingErrorListener.INSTANCE);
                    try {
                        final ParseTree tree = parser.parse();
                        final ParseTreeVisitor<Expression> visitor = new ExpressionParseVisitor();
                        final Expression result = visitor.visit(tree);
                        if (result == null) {
                            throw new BadExpressionException("Failed to parse expression " + expr);
                        }
                        return result;
                    } catch (final ParseCancellationException e) {
                        throw new BadExpressionException(e.getMessage());
                    }
                }
            });

    private static class ThrowingErrorListener extends BaseErrorListener {

        public static final ThrowingErrorListener INSTANCE = new ThrowingErrorListener();

        @Override
        public void syntaxError(final Recognizer<?, ?> recognizer, final Object offendingSymbol, final int line,
                                final int charPositionInLine, final String msg, final RecognitionException e) throws ParseCancellationException {

            throw new ParseCancellationException("line " + line + ":" + charPositionInLine + " " + msg);
        }
    }

    public static ExpressionCache getDefault() {

        return new ExpressionCache();
    }

    public Expression parse(final String expr) {

        try {
            return cache.getUnchecked(expr);
        } catch (final UncheckedExecutionException e) {
            throw Throwables.toRuntimeCause(e.getCause());
        }
    }
}
