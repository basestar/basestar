package io.basestar.schema.expression;

import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.aggregate.*;
import io.basestar.expression.arithmetic.*;
import io.basestar.expression.bitwise.*;
import io.basestar.expression.call.Callable;
import io.basestar.expression.call.LambdaCall;
import io.basestar.expression.call.MemberCall;
import io.basestar.expression.compare.*;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.constant.NameConstant;
import io.basestar.expression.function.*;
import io.basestar.expression.iterate.*;
import io.basestar.expression.literal.LiteralArray;
import io.basestar.expression.literal.LiteralObject;
import io.basestar.expression.literal.LiteralSet;
import io.basestar.expression.logical.And;
import io.basestar.expression.logical.Not;
import io.basestar.expression.logical.Or;
import io.basestar.expression.methods.Methods;
import io.basestar.expression.text.Like;
import io.basestar.schema.use.*;
import io.basestar.util.Name;

import java.lang.reflect.Type;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InferenceVisitor implements ExpressionVisitor<Use<?>> {

    private final InferenceContext context;

    private final Methods methods;

    public InferenceVisitor(final InferenceContext context) {

        this(context, Methods.builder().defaults().build());
    }

    public InferenceVisitor(final InferenceContext context, final Methods methods) {

        this.context = context;
        this.methods = methods;
    }

    @Override
    public Use<?> visit(final Expression expression) {

        return expression.visit(this).optional(false);
    }

    @Override
    public Use<?> visitAdd(final Add expression) {

        final Use<?> lhs = visit(expression.getLhs());
        final Use<?> rhs = visit(expression.getRhs());
        if(lhs instanceof UseString || rhs instanceof UseString) {
            return UseString.DEFAULT;
        } else {
            return numericPromote(lhs, rhs);
        }
    }

    @Override
    public Use<?> visitDiv(final Div expression) {

        return numericPromote(visit(expression.getLhs()), visit(expression.getRhs()));
    }

    @Override
    public Use<?> visitMod(final Mod expression) {

        return numericPromote(visit(expression.getLhs()), visit(expression.getRhs()));
    }

    @Override
    public Use<?> visitMul(final Mul expression) {

        return numericPromote(visit(expression.getLhs()), visit(expression.getRhs()));
    }

    @Override
    public Use<?> visitNegate(final Negate expression) {

        return visit(expression.getOperand());
    }

    @Override
    public Use<?> visitPow(final Pow expression) {

        return numericPromote(visit(expression.getLhs()), visit(expression.getRhs()));
    }

    @Override
    public Use<?> visitSub(final Sub expression) {

        return numericPromote(visit(expression.getLhs()), visit(expression.getRhs()));
    }

    private Use<?> numericPromote(final Use<?> lhs, final Use<?> rhs) {

        if(lhs instanceof UseNumber || rhs instanceof UseNumber) {
            return UseNumber.DEFAULT;
        } else {
            return UseInteger.DEFAULT;
        }
    }

    @Override
    public Use<?> visitBitAnd(final BitAnd expression) {

        return UseInteger.DEFAULT;
    }

    @Override
    public Use<?> visitBitFlip(final BitNot expression) {

        return UseInteger.DEFAULT;
    }

    @Override
    public Use<?> visitBitLsh(final BitLsh expression) {

        return UseInteger.DEFAULT;
    }

    @Override
    public Use<?> visitBitOr(final BitOr expression) {

        return UseInteger.DEFAULT;
    }

    @Override
    public Use<?> visitBitRsh(final BitRsh expression) {

        return UseInteger.DEFAULT;
    }

    @Override
    public Use<?> visitBitXor(final BitXor expression) {

        return UseInteger.DEFAULT;
    }

    @Override
    public Use<?> visitCmp(final Cmp expression) {

        return UseInteger.DEFAULT;
    }

    @Override
    public Use<?> visitEq(final Eq expression) {

        return UseBoolean.DEFAULT;
    }

    @Override
    public Use<?> visitGt(final Gt expression) {

        return UseBoolean.DEFAULT;
    }

    @Override
    public Use<?> visitGte(final Gte expression) {

        return UseBoolean.DEFAULT;
    }

    @Override
    public Use<?> visitLt(final Lt expression) {

        return UseBoolean.DEFAULT;
    }

    @Override
    public Use<?> visitLte(final Lte expression) {

        return UseBoolean.DEFAULT;
    }

    @Override
    public Use<?> visitNe(final Ne expression) {

        return UseBoolean.DEFAULT;
    }

    @Override
    public Use<?> visitConstant(final Constant expression) {

        final Object value = expression.getValue();
        return constantType(value);
    }

    private static Use<?> constantType(final Object value) {

        if(value == null) {
            return UseAny.DEFAULT;
        } else if(value instanceof Set) {
            final Stream<Use<?>> values = ((Collection<?>) value).stream()
                    .filter(Objects::nonNull).map(InferenceVisitor::constantType);
            return UseSet.from(values.reduce(Use::commonBase).orElse(UseAny.DEFAULT));
        } else if(value instanceof Collection) {
            final Stream<Use<?>> values = ((Collection<?>) value).stream()
                    .filter(Objects::nonNull).map(InferenceVisitor::constantType);
            return UseArray.from(values.reduce(Use::commonBase).orElse(UseAny.DEFAULT));
        } else if(value instanceof Map) {
            final Stream<Use<?>> values = ((Map<?, ?>) value).values().stream()
                    .filter(Objects::nonNull).map(InferenceVisitor::constantType);
            return UseMap.from(values.reduce(Use::commonBase).orElse(UseAny.DEFAULT));
        } else {
            return Use.fromJavaType(value.getClass());
        }
    }

    @Override
    public Use<?> visitNameConstant(final NameConstant expression) {

        final Name name = expression.getName();
        return context.typeOf(name);
    }

    @Override
    public Use<?> visitCoalesce(final Coalesce expression) {

        return Use.commonBase(visit(expression.getLhs()), visit(expression.getRhs()));
    }

    @Override
    public Use<?> visitIfElse(final IfElse expression) {

        return Use.commonBase(visit(expression.getThen()), visit(expression.getOtherwise()));
    }

    @Override
    public Use<?> visitIn(final In expression) {

        return UseBoolean.DEFAULT;
    }

    @Override
    public Use<?> visitIndex(final Index expression) {

        final Use<?> lhs = visit(expression.getLhs());
        if(lhs instanceof UseCollection) {
            return ((UseCollection<?, ?>)lhs).getType();
        } else if(lhs instanceof UseMap<?>) {
            return ((UseMap<?>)lhs).getType();
        } else {
            throw new UnsupportedOperationException("Cannot infer type of " + expression);
        }
    }

    @Override
    public Use<?> visitLambda(final Lambda expression) {

        return UseAny.DEFAULT;
    }

    @Override
    public Use<?> visitLambdaCall(final LambdaCall expression) {

        return UseAny.DEFAULT;
    }

    @Override
    public Use<?> visitMemberCall(final MemberCall expression) {

        final Use<?> with = visit(expression.getWith());
        final String member = expression.getMember();
        final List<Use<?>> args = expression.getArgs().stream()
                .map(this::visit).collect(Collectors.toList());
        return typeOfCall(with, member, args);
    }

    @Override
    public Use<?> visitMember(final Member expression) {

        final Use<?> with = visit(expression.getWith());
        final String member = expression.getMember();
        if(with != null) {
            return with.typeOf(Name.of(member));
        } else {
            throw new UnsupportedOperationException("Cannot infer type of " + expression);
        }
    }

    @Override
    public Use<?> visitWith(final With expression) {

        final Map<String, Use<?>> with = expression.getWith().entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> visit(e.getValue())
        ));
        final InferenceContext newContext = context.with(with);
        return new InferenceVisitor(newContext, methods).visit(expression.getYield());
    }

    @Override
    public Use<?> visitForAll(final ForAll expression) {

        return UseBoolean.DEFAULT;
    }

    @Override
    public Use<?> visitForAny(final ForAny expression) {

        return UseBoolean.DEFAULT;
    }

    @Override
    public Use<?> visitForArray(final ForArray expression) {

        return UseArray.DEFAULT;
    }

    @Override
    public Use<?> visitForObject(final ForObject expression) {

        return UseMap.DEFAULT;
    }

    @Override
    public Use<?> visitForSet(final ForSet expression) {

        return UseSet.DEFAULT;
    }

    @Override
    public Use<?> visitOf(final Of expression) {

        // FIXME: this is probably wrong
        return UseArray.from(visit(expression.getExpr()));
    }

    @Override
    public Use<?> visitWhere(final Where expression) {

        return visit(expression.getLhs());
    }

    @Override
    public Use<?> visitLiteralArray(final LiteralArray expression) {

        final Stream<Use<?>> args = expression.getArgs().stream().map(this::visit);
        return UseArray.from(args.reduce(Use::commonBase).orElse(UseAny.DEFAULT));
    }

    @Override
    public Use<?> visitLiteralObject(final LiteralObject expression) {

        final Stream<Use<?>> args = expression.getArgs().values().stream().map(this::visit);
        return UseMap.from(args.reduce(Use::commonBase).orElse(UseAny.DEFAULT));
    }

    @Override
    public Use<?> visitLiteralSet(final LiteralSet expression) {

        final Stream<Use<?>> args = expression.getArgs().stream().map(this::visit);
        return UseSet.from(args.reduce(Use::commonBase).orElse(UseAny.DEFAULT));
    }

    @Override
    public Use<?> visitAnd(final And expression) {

        return UseBoolean.DEFAULT;
    }

    @Override
    public Use<?> visitNot(final Not expression) {

        return UseBoolean.DEFAULT;
    }

    @Override
    public Use<?> visitOr(final Or expression) {

        return UseBoolean.DEFAULT;
    }

    @Override
    public Use<?> visitLike(final Like like) {

        return UseBoolean.DEFAULT;
    }

    @Override
    public Use<?> visitAvg(final Avg expression) {

        return UseNumber.DEFAULT;
    }

    @Override
    public Use<?> visitCollectArray(final CollectArray expression) {

        return UseArray.from(visit(expression.getInput()));
    }

    @Override
    public Use<?> visitCount(final Count expression) {

        return UseInteger.DEFAULT;
    }

    @Override
    public Use<?> visitMax(final Max expression) {

        return visit(expression.getInput());
    }

    @Override
    public Use<?> visitMin(final Min expression) {

        return visit(expression.getInput());
    }

    @Override
    public Use<?> visitSum(final Sum expression) {

        return visit(expression.getInput());
    }

    protected Use<?> typeOfCall(final Use<?> target, final String member, final List<Use<?>> args) {

        final Type[] argTypes = args.stream().map(Use::javaType).toArray(Type[]::new);
        final Callable callable = methods.callable(target.javaType(), member, argTypes);
        return Use.fromJavaType(callable.type());
    }
}
