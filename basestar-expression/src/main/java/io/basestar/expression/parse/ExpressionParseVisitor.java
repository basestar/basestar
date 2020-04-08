package io.basestar.expression.parse;

/*-
 * #%L
 * basestar-expression
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2020 basestar.io
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

import io.basestar.expression.Expression;
import io.basestar.expression.arithmetic.Add;
import io.basestar.expression.arithmetic.Div;
import io.basestar.expression.arithmetic.Mod;
import io.basestar.expression.arithmetic.Mul;
import io.basestar.expression.arithmetic.*;
import io.basestar.expression.arithmetic.Pow;
import io.basestar.expression.arithmetic.Sub;
import io.basestar.expression.bitwise.BitAnd;
import io.basestar.expression.bitwise.BitLsh;
import io.basestar.expression.bitwise.BitNot;
import io.basestar.expression.bitwise.BitOr;
import io.basestar.expression.bitwise.BitRsh;
import io.basestar.expression.bitwise.BitXor;
import io.basestar.expression.compare.Cmp;
import io.basestar.expression.compare.Eq;
import io.basestar.expression.compare.Gt;
import io.basestar.expression.compare.Gte;
import io.basestar.expression.compare.Lt;
import io.basestar.expression.compare.Lte;
import io.basestar.expression.compare.Ne;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.constant.PathConstant;
import io.basestar.expression.function.*;
import io.basestar.expression.function.In;
import io.basestar.expression.function.With;
import io.basestar.expression.iterate.*;
import io.basestar.expression.iterate.Of;
import io.basestar.expression.iterate.Where;
import io.basestar.expression.literal.LiteralArray;
import io.basestar.expression.literal.LiteralObject;
import io.basestar.expression.literal.LiteralSet;
import io.basestar.expression.logical.And;
import io.basestar.expression.logical.Not;
import io.basestar.expression.logical.Or;
import io.basestar.expression.parse.ExpressionParser.*;
import io.basestar.expression.type.Values;
import lombok.RequiredArgsConstructor;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.tree.AbstractParseTreeVisitor;

import java.lang.String;
import java.util.*;
import java.util.stream.Collectors;

import static io.basestar.expression.parse.ExpressionLexer.Add;
import static io.basestar.expression.parse.ExpressionLexer.BitLsh;
import static io.basestar.expression.parse.ExpressionLexer.BitNot;
import static io.basestar.expression.parse.ExpressionLexer.BitRsh;
import static io.basestar.expression.parse.ExpressionLexer.Div;
import static io.basestar.expression.parse.ExpressionLexer.Eq;
import static io.basestar.expression.parse.ExpressionLexer.Gt;
import static io.basestar.expression.parse.ExpressionLexer.Gte;
import static io.basestar.expression.parse.ExpressionLexer.Lt;
import static io.basestar.expression.parse.ExpressionLexer.Lte;
import static io.basestar.expression.parse.ExpressionLexer.Mod;
import static io.basestar.expression.parse.ExpressionLexer.Mul;
import static io.basestar.expression.parse.ExpressionLexer.Ne;
import static io.basestar.expression.parse.ExpressionLexer.Not;
import static io.basestar.expression.parse.ExpressionLexer.Sub;

@RequiredArgsConstructor
public class ExpressionParseVisitor extends AbstractParseTreeVisitor<Expression> implements ExpressionVisitor<Expression> {

    @Override
    public Expression visitParse(final ParseContext ctx) {

        return visit(ctx.expr());
    }

    @Override
    public Expression visitExprs(final ExprsContext ctx) {

        return new LiteralArray(visit(ctx.expr()));
    }

    @Override
    public Expression visitPair(final PairContext ctx) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitAs(final AsContext ctx) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitName(final NameContext ctx) {

        throw new UnsupportedOperationException();
    }

//    @Override
//    public Expression visitPath(final ExprPathContext ctx) {
//
//        throw new UnsupportedOperationException();
//    }

    @Override
    public Expression visitExprMember(final ExprMemberContext ctx) {

        final Expression with = visit(ctx.expr());
        final String property = ctx.Identifier().getText();
        return new Member(with, property);
    }

    @Override
    public Expression visitExprStarMember(final ExprStarMemberContext ctx) {

        final Expression with = visit(ctx.expr());
        final String property = ctx.Identifier().getText();
        return new StarMember(with, property);
    }

    @Override
    public Expression visitExprCall(final ExprCallContext ctx) {

        final Expression with = visit(ctx.expr());
        final List<Expression> args = ctx.exprs() == null ? Collections.emptyList() : visit(ctx.exprs().expr());
        if (ctx.Identifier() != null) {
            final String method = ctx.Identifier().getText();
            return new MemberCall(with, method, args);
        } else {
            return new LambdaCall(with, args);
        }
    }

    @Override
    public Expression visitExprIndex(final ExprIndexContext ctx) {

        final Expression with = visit(ctx.expr(0));
        final Expression index = visit(ctx.expr(1));
        return new Index(with, index);
    }

    @Override
    public Expression visitExprIn(final ExprInContext ctx) {

        return new In(visit(ctx.expr(0)), visit(ctx.expr(1)));
    }

    @Override
    public Expression visitExprMul(final ExprMulContext ctx) {

        switch (ctx.op.getType()) {
            case Mul:
                return new Mul(visit(ctx.expr(0)), visit(ctx.expr(1)));
            case Div:
                return new Div(visit(ctx.expr(0)), visit(ctx.expr(1)));
            case Mod:
                return new Mod(visit(ctx.expr(0)), visit(ctx.expr(1)));
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    public Expression visitExprExpr(final ExprExprContext ctx) {

        return visit(ctx.expr());
    }

    @Override
    public Expression visitExprOf(final ExprOfContext ctx) {

        final List<String> as = ctx.name().stream().map(RuleContext::getText).collect(Collectors.toList());
        final Expression with = visit(ctx.expr());
        if(as.size() == 1) {
            return new Of(as.get(0), with);
        } else if(as.size() == 2) {
            return new Of(as.get(0), as.get(1), with);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public Expression visitExprWhere(final ExprWhereContext ctx) {

        return new Where(visit(ctx.expr(0)), visit(ctx.expr(1)));
    }

    @Override
    public Expression visitExprForObject(final ExprForObjectContext ctx) {

        final Expression yieldKey = visit(ctx.expr(0));
        final Expression yieldValue = visit(ctx.expr(1));
        final Expression with = visit(ctx.expr(2));
        return new ForObject(yieldKey, yieldValue, with);
    }

    @Override
    public Expression visitExprForArray(final ExprForArrayContext ctx) {

        final Expression yield = visit(ctx.expr(0));
        final Expression with = visit(ctx.expr(1));
        return new ForArray(yield, with);
    }

    @Override
    public Expression visitExprForSet(final ExprForSetContext ctx) {

        final Expression yield = visit(ctx.expr(0));
        final Expression with = visit(ctx.expr(1));
        return new ForSet(yield, with);
    }


    @Override
    public Expression visitExprForAll(final ExprForAllContext ctx) {

        final Expression yield = visit(ctx.expr(0));
        final Expression with = visit(ctx.expr(1));
        return new ForAll(yield, with);
    }

    @Override
    public Expression visitExprForAny(final ExprForAnyContext ctx) {

        final Expression yield = visit(ctx.expr(0));
        final Expression with = visit(ctx.expr(1));
        return new ForAny(yield, with);
    }

    @Override
    public Expression visitExprCoalesce(final ExprCoalesceContext ctx) {

        return new Coalesce(visit(ctx.expr(0)), visit(ctx.expr(1)));
    }

    @Override
    public Expression visitExprRel(final ExprRelContext ctx) {

        switch (ctx.op.getType()) {
            case Gt:
                return new Gt(visit(ctx.expr(0)), visit(ctx.expr(1)));
            case Gte:
                return new Gte(visit(ctx.expr(0)), visit(ctx.expr(1)));
            case Lt:
                return new Lt(visit(ctx.expr(0)), visit(ctx.expr(1)));
            case Lte:
                return new Lte(visit(ctx.expr(0)), visit(ctx.expr(1)));
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    public Expression visitExprUnary(final ExprUnaryContext ctx) {

        switch (ctx.op.getType()) {
            case Sub:
                return new Negate(visit(ctx.expr()));
            case Not:
                return new Not(visit(ctx.expr()));
            case BitNot:
                return new BitNot(visit(ctx.expr()));
            default:
                throw new UnsupportedOperationException();
        }
    }


    @Override
    public Expression visitExprAdd(final ExprAddContext ctx) {

        switch (ctx.op.getType()) {
            case Add:
                return new Add(visit(ctx.expr(0)), visit(ctx.expr(1)));
            case Sub:
                return new Sub(visit(ctx.expr(0)), visit(ctx.expr(1)));
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    public Expression visitExprVar(final ExprVarContext ctx) {

        return new PathConstant(ctx.Identifier().getText());
    }

    @Override
    public Expression visitExprObject(final ExprObjectContext ctx) {

        return new LiteralObject(
                ctx.pair().stream().collect(Collectors.toMap(
                        pair -> visit(pair.expr(0)),
                        pair -> visit(pair.expr(1))
                ))
        );
    }

    @Override
    public Expression visitExprEq(final ExprEqContext ctx) {

        switch (ctx.op.getType()) {
            case Eq:
                return new Eq(visit(ctx.expr(0)), visit(ctx.expr(1)));
            case Ne:
                return new Ne(visit(ctx.expr(0)), visit(ctx.expr(1)));
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    public Expression visitExprNumber(final ExprNumberContext ctx) {

        final String text = ctx.Number().getText();
        return new Constant(Values.parseNumber(text));
    }

    @Override
    public Expression visitExprLambda(final ExprLambdaContext ctx) {

        final List<String> args = ctx.name().stream().map(NameContext::getText).collect(Collectors.toList());
        return new Lambda(args, visit(ctx.expr()));
    }

    @Override
    public Expression visitExprNull(final ExprNullContext ctx) {

        return new Constant(null);
    }

    @Override
    public Expression visitExprOr(final ExprOrContext ctx) {

        return new Or(visit(ctx.expr(0)), visit(ctx.expr(1)));
    }

    @Override
    public Expression visitExprString(final ExprStringContext ctx) {

        final String text = ctx.String().getText();
        final String processedText = text.substring(1, text.length() - 1)
                .replaceAll("\\\\(.)", "$1");
        return new Constant(processedText);
    }

    @Override
    public Expression visitExprBool(final ExprBoolContext ctx) {

        final String text = ctx.getText();
        return new Constant(Boolean.valueOf(text));
    }

    @Override
    public Expression visitExprArray(final ExprArrayContext ctx) {

        return ctx.exprs() == null ? new Constant(Collections.emptyList()) : visit(ctx.exprs());
    }

    @Override
    public Expression visitExprSet(final ExprSetContext ctx) {

        return ctx.exprs() == null ? new Constant(Collections.emptySet()) : new LiteralSet(visit(ctx.exprs().expr()));
    }

    @Override
    public Expression visitExprAnd(final ExprAndContext ctx) {

        return new And(visit(ctx.expr(0)), visit(ctx.expr(1)));
    }

    @Override
    public Expression visitExprPow(final ExprPowContext ctx) {

        return new Pow(visit(ctx.expr(0)), visit(ctx.expr(1)));
    }

    @Override
    public Expression visitExprIfElse(final ExprIfElseContext ctx) {

        final List<ExprContext> exprs = ctx.expr();
        final Expression with = visit(exprs.get(0));
        final Expression then = visit(exprs.get(1));
        final Expression otherwise = visit(exprs.get(2));
        return new IfElse(with, then, otherwise);
    }

    @Override
    public Expression visitExprBitXor(final ExprBitXorContext ctx) {

        return new BitXor(visit(ctx.expr(0)), visit(ctx.expr(1)));
    }

    @Override
    public Expression visitExprCmp(final ExprCmpContext ctx) {

        return new Cmp(visit(ctx.expr(0)), visit(ctx.expr(1)));
    }

    @Override
    public Expression visitExprBitAnd(final ExprBitAndContext ctx) {

        return new BitAnd(visit(ctx.expr(0)), visit(ctx.expr(1)));
    }

    @Override
    public Expression visitExprBitSh(final ExprBitShContext ctx) {

        switch (ctx.op.getType()) {
            case BitLsh:
                return new BitLsh(visit(ctx.expr(0)), visit(ctx.expr(1)));
            case BitRsh:
                return new BitRsh(visit(ctx.expr(0)), visit(ctx.expr(1)));
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    public Expression visitExprBitOr(final ExprBitOrContext ctx) {

        return new BitOr(visit(ctx.expr(0)), visit(ctx.expr(1)));
    }

    @Override
    public Expression visitExprWith(final ExprWithContext ctx) {

        final Map<String, Expression> set = new LinkedHashMap<>();
        for (final AsContext as : ctx.as()) {
            set.put(as.name().getText(), visit(as.expr()));
        }
        final Expression yield = visit(ctx.expr());
        return new With(set, yield);
    }

    private List<Expression> visit(final List<ExprContext> exprs) {

        final List<Expression> content = new ArrayList<>();
        for (final ParserRuleContext expr : exprs) {
            content.add(visit(expr));
        }
        return content;
    }
}
