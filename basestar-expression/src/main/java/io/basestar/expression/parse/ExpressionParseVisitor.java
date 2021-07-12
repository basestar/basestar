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

import io.basestar.expression.Expression;
import io.basestar.expression.aggregate.Aggregate;
import io.basestar.expression.arithmetic.Add;
import io.basestar.expression.arithmetic.Div;
import io.basestar.expression.arithmetic.Mod;
import io.basestar.expression.arithmetic.Mul;
import io.basestar.expression.arithmetic.Sub;
import io.basestar.expression.arithmetic.*;
import io.basestar.expression.bitwise.BitLsh;
import io.basestar.expression.bitwise.BitRsh;
import io.basestar.expression.bitwise.*;
import io.basestar.expression.call.LambdaCall;
import io.basestar.expression.call.MemberCall;
import io.basestar.expression.compare.Cmp;
import io.basestar.expression.compare.Eq;
import io.basestar.expression.compare.Gt;
import io.basestar.expression.compare.Gte;
import io.basestar.expression.compare.Lt;
import io.basestar.expression.compare.Lte;
import io.basestar.expression.compare.*;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.constant.NameConstant;
import io.basestar.expression.function.Case;
import io.basestar.expression.function.In;
import io.basestar.expression.function.With;
import io.basestar.expression.function.*;
import io.basestar.expression.iterate.*;
import io.basestar.expression.literal.LiteralArray;
import io.basestar.expression.literal.LiteralObject;
import io.basestar.expression.literal.LiteralSet;
import io.basestar.expression.logical.And;
import io.basestar.expression.logical.Not;
import io.basestar.expression.logical.Or;
import io.basestar.expression.parse.ExpressionParser.*;
import io.basestar.expression.sql.From;
import io.basestar.expression.sql.Select;
import io.basestar.expression.sql.Sql;
import io.basestar.expression.sql.Union;
import io.basestar.expression.text.ILike;
import io.basestar.expression.text.SLike;
import io.basestar.util.*;
import lombok.RequiredArgsConstructor;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.tree.AbstractParseTreeVisitor;
import org.antlr.v4.runtime.tree.ParseTree;

import java.lang.String;
import java.util.*;
import java.util.stream.Collectors;

import static io.basestar.expression.parse.ExpressionLexer.Add;
import static io.basestar.expression.parse.ExpressionLexer.Bang;
import static io.basestar.expression.parse.ExpressionLexer.BangEq;
import static io.basestar.expression.parse.ExpressionLexer.BitLsh;
import static io.basestar.expression.parse.ExpressionLexer.BitRsh;
import static io.basestar.expression.parse.ExpressionLexer.Desc;
import static io.basestar.expression.parse.ExpressionLexer.Div;
import static io.basestar.expression.parse.ExpressionLexer.Eq;
import static io.basestar.expression.parse.ExpressionLexer.EqEq;
import static io.basestar.expression.parse.ExpressionLexer.Gt;
import static io.basestar.expression.parse.ExpressionLexer.Gte;
import static io.basestar.expression.parse.ExpressionLexer.ILike;
import static io.basestar.expression.parse.ExpressionLexer.Like;
import static io.basestar.expression.parse.ExpressionLexer.Lt;
import static io.basestar.expression.parse.ExpressionLexer.Lte;
import static io.basestar.expression.parse.ExpressionLexer.Mod;
import static io.basestar.expression.parse.ExpressionLexer.Mul;
import static io.basestar.expression.parse.ExpressionLexer.Not;
import static io.basestar.expression.parse.ExpressionLexer.Sub;
import static io.basestar.expression.parse.ExpressionLexer.Tilde;

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
    public Expression visitIdentifier(final IdentifierContext ctx) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitName(final NameContext ctx) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitNames(final NamesContext ctx) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitWhere(final WhereContext ctx) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitOf(final OfContext ctx) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitTypeExpr(final TypeExprContext ctx) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitWithExprs(final WithExprsContext ctx) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitWithExpr(final WithExprContext ctx) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitCaseExpr(final CaseExprContext ctx) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitSelectAll(final SelectAllContext ctx) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitSelectAnon(final SelectAnonContext ctx) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitSelectNamed(final SelectNamedContext ctx) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitSelectExprs(final SelectExprsContext ctx) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitFromAnon(final FromAnonContext ctx) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitFromNamed(final FromNamedContext ctx) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitFromInnerJoin(final FromInnerJoinContext ctx) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitFromLeftOuterJoin(final FromLeftOuterJoinContext ctx) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitFromRightOuterJoin(final FromRightOuterJoinContext ctx) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitFromFullOuterJoin(final FromFullOuterJoinContext ctx) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitFromExprs(final FromExprsContext ctx) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitUnionDistinct(final UnionDistinctContext ctx) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitUnionAll(final UnionAllContext ctx) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitSorts(final SortsContext ctx) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitSort(final SortContext ctx) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Expression visitExprMember(final ExprMemberContext ctx) {

        final Expression with = visit(ctx.expr());
        final String property = ctx.Identifier().getText();
        return new Member(with, property);
    }

    @Override
    public Expression visitExprCall(final ExprCallContext ctx) {

        final Expression with = visit(ctx.expr());
        final List<Expression> args = ctx.exprs() == null ? Collections.emptyList() : visit(ctx.exprs().expr());
        if (ctx.Identifier() != null) {
            final String method = ctx.Identifier().getText();
            return new MemberCall(with, method, args);
        } else {
            if(with instanceof NameConstant) {
                final Name name = ((NameConstant) with).getName();
                if(name.size() == 1) {
                    final Aggregate.Factory factory = Aggregate.factory(name.first());
                    if(factory != null) {
                        return factory.create(args);
                    }
                }
            }
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
    public Expression visitExprCase(final ExprCaseContext ctx) {

        final List<Pair<Expression, Expression>> exprs = new ArrayList<>();
        for(final CaseExprContext c : ctx.caseExpr()) {
            exprs.add(Pair.of(visit(c.expr(0)), visit(c.expr(1))));
        }
        final Expression otherwise;
        if(ctx.expr() != null) {
            otherwise = visit(ctx.expr());
        } else {
            otherwise = null;
        }
        return new Case(exprs, otherwise);
    }

    protected ContextIterator iterator(final OfContext ctx) {

        final List<String> as = ctx.identifier().stream().map(RuleContext::getText).collect(Collectors.toList());
        final Expression with = visit(ctx.expr());
        final Expression where = Nullsafe.map(ctx.where(), w -> visit(w.expr()));
        final ContextIterator iter;
        if(as.size() == 1) {
            iter = new ContextIterator.OfValue(as.get(0), with);
        } else if(as.size() == 2) {
            iter = new ContextIterator.OfKeyValue(as.get(0), as.get(1), with);
        } else {
            throw new UnsupportedOperationException();
        }
        if(where != null) {
            return  new ContextIterator.Where(iter, where);
        } else {
            return iter;
        }
    }

    @Override
    public Expression visitExprForObject(final ExprForObjectContext ctx) {

        final Expression yieldKey = visit(ctx.expr(0));
        final Expression yieldValue = visit(ctx.expr(1));
        final ContextIterator of = iterator(ctx.of());
        return new ForObject(yieldKey, yieldValue, of);
    }

    @Override
    public Expression visitExprForArray(final ExprForArrayContext ctx) {

        final Expression yield = visit(ctx.expr());
        final ContextIterator of = iterator(ctx.of());
        return new ForArray(yield, of);
    }

    @Override
    public Expression visitExprForSet(final ExprForSetContext ctx) {

        final Expression yield = visit(ctx.expr());
        final ContextIterator of = iterator(ctx.of());
        return new ForSet(yield, of);
    }

    @Override
    public Expression visitExprForAll(final ExprForAllContext ctx) {

        final Expression yield = visit(ctx.expr());
        final ContextIterator of = iterator(ctx.of());
        return new ForAll(yield, of);
    }

    @Override
    public Expression visitExprForAny(final ExprForAnyContext ctx) {

        final Expression yield = visit(ctx.expr());
        final ContextIterator of = iterator(ctx.of());
        return new ForAny(yield, of);
    }

    @Override
    public Expression visitExprLike(final ExprLikeContext ctx) {

        switch (ctx.op.getType()) {
            case Like:
                return new SLike(visit(ctx.expr(0)), visit(ctx.expr(1)));
            case ILike:
                return new ILike(visit(ctx.expr(0)), visit(ctx.expr(1)));
            default:
                throw new UnsupportedOperationException();
        }
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
            case Bang:
                return new Not(visit(ctx.expr()));
            case Tilde:
                return new BitNot(visit(ctx.expr()));
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    public Expression visitExprWith(final ExprWithContext ctx) {

        final Map<String, Expression> with = new HashMap<>();
        final Expression yield = visit(ctx.expr());
        for(final WithExprContext c : ctx.withExprs().withExpr()) {
            with.put(c.identifier().getText(), visit(c.expr()));
        }
        return new With(with, yield);
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
    public Expression visitExprNameConstant(final ExprNameConstantContext ctx) {

        final Name name = name(ctx.name());
        return new NameConstant(name);
    }

    private List<Name> names(final NamesContext ctx) {

        return ctx == null ? Immutable.list() : ctx.name().stream().map(this::name).collect(Collectors.toList());
    }

    private Name name(final NameContext ctx) {

        return Name.of(ctx.Identifier().stream().map(ParseTree::getText).toArray(java.lang.String[]::new));
    }

    private List<Sort> sorts(final SortsContext ctx) {

        return ctx == null ? Immutable.list() : ctx.sort().stream().map(this::sort).collect(Collectors.toList());
    }

    private Sort sort(final SortContext ctx) {

        final Name name = name(ctx.name());
        if(ctx.order != null && Desc == ctx.order.getType()) {
            return Sort.desc(name);
        } else {
            return Sort.asc(name);
        }
    }

    private String identifier(final IdentifierContext ctx) {

        return ctx.Identifier().getText();
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

    private io.basestar.expression.sql.Select visitSelection(final SelectExprContext ctx) {

        if(ctx instanceof SelectAnonContext) {
            final SelectAnonContext anon = (SelectAnonContext)ctx;
            final Expression expr = visit(anon.expr());
            return new Select.Anonymous(expr);
        } else if(ctx instanceof SelectNamedContext) {
            final SelectNamedContext named = (SelectNamedContext)ctx;
            final Expression expr = visit(named.expr());
            final String name = named.identifier().getText();
            return new Select.Named(expr, name);
        } else if(ctx instanceof SelectAllContext) {
            return new Select.All();
        } else {
            throw new UnsupportedOperationException("Unexpected " + ctx);
        }
    }

    private io.basestar.expression.sql.From visitFrom(final FromExprContext ctx) {

        if(ctx instanceof FromAnonContext) {
            final FromAnonContext anon = (FromAnonContext)ctx;
            final Expression expr = visit(anon.expr());
            return new From.Anonymous(expr);
        } else if(ctx instanceof FromNamedContext) {
            final FromNamedContext named = (FromNamedContext)ctx;
            final Expression expr = visit(named.expr());
            final String name = named.identifier().getText();
            return new From.Named(expr, name);
        } else if(ctx instanceof FromInnerJoinContext) {
            final FromInnerJoinContext join = (FromInnerJoinContext)ctx;
            return fromJoin(From.Join.Type.INNER, join.fromExpr(0), join.fromExpr(1), join.expr());
        } else if(ctx instanceof FromLeftOuterJoinContext) {
            final FromLeftOuterJoinContext join = (FromLeftOuterJoinContext)ctx;
            return fromJoin(From.Join.Type.LEFT_OUTER, join.fromExpr(0), join.fromExpr(1), join.expr());
        } else if(ctx instanceof FromRightOuterJoinContext) {
            final FromRightOuterJoinContext join = (FromRightOuterJoinContext)ctx;
            return fromJoin(From.Join.Type.RIGHT_OUTER, join.fromExpr(0), join.fromExpr(1), join.expr());
        } else if(ctx instanceof FromFullOuterJoinContext) {
            final FromFullOuterJoinContext join = (FromFullOuterJoinContext)ctx;
            return fromJoin(From.Join.Type.FULL_OUTER, join.fromExpr(0), join.fromExpr(1), join.expr());
        } else {
            throw new UnsupportedOperationException("Unexpected " + ctx);
        }
    }

    private io.basestar.expression.sql.From fromJoin(final From.Join.Type type, final FromExprContext leftExpr, final FromExprContext rightExpr, final ExprContext onExpr) {

        final io.basestar.expression.sql.From left = visitFrom(leftExpr);
        final io.basestar.expression.sql.From right = visitFrom(rightExpr);
        final Expression on = visit(onExpr);
        return new From.Join(left, right, on, type);
    }

    private io.basestar.expression.sql.Union visitUnion(final UnionExprContext ctx) {

        if(ctx instanceof UnionDistinctContext) {
            final UnionDistinctContext distinct = (UnionDistinctContext)ctx;
            final Expression expr = visit(distinct.expr());
            return new Union.Distinct(expr);
        } else if(ctx instanceof UnionAllContext) {
            final UnionAllContext all = (UnionAllContext) ctx;
            final Expression expr = visit(all.expr());
            return new Union.All(expr);
        } else {
            throw new UnsupportedOperationException("Unexpected " + ctx);
        }
    }

    @Override
    public Expression visitExprSelect(final ExprSelectContext ctx) {

        final List<io.basestar.expression.sql.Select> select = new ArrayList<>();
        for(final SelectExprContext c : ctx.selectExprs().selectExpr()) {
            select.add(visitSelection(c));
        }
        final List<io.basestar.expression.sql.From> from = new ArrayList<>();
        for(final FromExprContext c : ctx.fromExprs().fromExpr()) {
            from.add(visitFrom(c));
        }
        final Expression where = ctx.expr() == null ? null : visit(ctx.expr());
        final List<Expression> group = ctx.exprs() == null ? null : visit(ctx.exprs().expr());
        final List<Sort> order = sorts(ctx.sorts());
        final List<io.basestar.expression.sql.Union> union = new ArrayList<>();
        for(final UnionExprContext c : ctx.unionExpr()) {
            union.add(visitUnion(c));
        }
        return new Sql(select, from, where, group, order, union);
    }

    @Override
    public Expression visitExprEq(final ExprEqContext ctx) {

        switch (ctx.op.getType()) {
            case Eq:
            case EqEq:
                return new Eq(visit(ctx.expr(0)), visit(ctx.expr(1)));
            case BangEq:
                return new Ne(visit(ctx.expr(0)), visit(ctx.expr(1)));
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    public Expression visitExprNumber(final ExprNumberContext ctx) {

        final String text = ctx.Number().getText();
        return Constant.valueOf(Text.parseNumber(text));
    }

    @Override
    public Expression visitExprLambda(final ExprLambdaContext ctx) {

        final List<String> args = ctx.identifier().stream().map(RuleContext::getText).collect(Collectors.toList());
        return new Lambda(args, visit(ctx.expr()));
    }

    @Override
    public Expression visitExprNull(final ExprNullContext ctx) {

        return Constant.NULL;
    }

    @Override
    public Expression visitExprOr(final ExprOrContext ctx) {

        return new Or(visit(ctx.expr(0)), visit(ctx.expr(1)));
    }

    @Override
    public Expression visitExprCast(final ExprCastContext ctx) {

        final Expression expr = visit(ctx.expr());
        final String type = ctx.typeExpr().identifier().toString();
        return new io.basestar.expression.function.Cast(expr, type);
    }

    @Override
    public Expression visitExprString(final ExprStringContext ctx) {

        final String text = ctx.String().getText();
        final String processedText = text.substring(1, text.length() - 1)
                .replaceAll("\\\\(.)", "$1");
        return Constant.valueOf(processedText);
    }

    @Override
    public Expression visitExprBool(final ExprBoolContext ctx) {

        final String text = ctx.getText();
        return Constant.valueOf(Boolean.valueOf(text));
    }

    @Override
    public Expression visitExprArray(final ExprArrayContext ctx) {

        return ctx.exprs() == null ? Constant.valueOf(Collections.emptyList()) : visit(ctx.exprs());
    }

    @Override
    public Expression visitExprSet(final ExprSetContext ctx) {

        return ctx.exprs() == null ? Constant.valueOf(Collections.emptySet()) : new LiteralSet(visit(ctx.exprs().expr()));
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

//    @Override
//    public Expression visitExprWith(final ExprWithContext ctx) {
//
//        final Map<String, Expression> set = new LinkedHashMap<>();
//        for (final AsContext as : ctx.as()) {
//            set.put(as.identifier().getText(), visit(as.expr()));
//        }
//        final Expression yield = visit(ctx.expr());
//        return new With(set, yield);
//    }

    private List<Expression> visit(final List<ExprContext> exprs) {

        final List<Expression> content = new ArrayList<>();
        for (final ExprContext expr : exprs) {
            content.add(visit(expr));
        }
        return content;
    }
}
