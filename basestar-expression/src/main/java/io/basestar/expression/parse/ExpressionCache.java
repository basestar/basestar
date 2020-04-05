package io.basestar.expression.parse;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.basestar.expression.Expression;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

import javax.annotation.Nonnull;

public class ExpressionCache {

    private final LoadingCache<String, Expression> cache = CacheBuilder.newBuilder()
            .build(new CacheLoader<String, Expression>() {
                @Override
                public Expression load(@Nonnull final String expr) {

                    final ExpressionLexer lexer = new ExpressionLexer(CharStreams.fromString(expr));
                    final CommonTokenStream tokens = new CommonTokenStream(lexer);
                    final ExpressionParser parser = new ExpressionParser(tokens);
                    final ParseTree tree = parser.parse();
                    final ParseTreeVisitor<Expression> visitor = new ExpressionParseVisitor();
                    return visitor.visit(tree);
                }
            });

    public static ExpressionCache getDefault() {

        return new ExpressionCache();
    }

    public Expression parse(final String expr) {

        return cache.getUnchecked(expr);
    }
}
