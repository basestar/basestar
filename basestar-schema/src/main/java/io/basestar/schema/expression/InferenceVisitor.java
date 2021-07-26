package io.basestar.schema.expression;

import com.google.common.collect.ImmutableMap;
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
import io.basestar.expression.sql.Sql;
import io.basestar.expression.text.Like;
import io.basestar.expression.type.DecimalContext;
import io.basestar.schema.use.*;
import io.basestar.util.Name;
import io.basestar.util.Optionals;
import io.basestar.util.Pair;

import java.lang.reflect.Type;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InferenceVisitor implements ExpressionVisitor<Optional<Use<?>>> {

    private final InferenceContext context;

    private final Methods methods;

    public InferenceVisitor(final InferenceContext context) {

        this(context, Methods.builder().defaults().build());
    }

    public InferenceVisitor(final InferenceContext context, final Methods methods) {

        this.context = context;
        this.methods = methods;
    }

    public Use<?> typeOf(final Expression expression) {

        return optionalTypeOf(expression).orElse(UseAny.DEFAULT);
    }

    public Optional<Use<?>> optionalTypeOf(final Expression expression) {

        return visit(expression);
    }

    @Override
    public Optional<Use<?>> visit(final Expression expression) {

        if(expression == null) {
            throw new IllegalStateException("Expression cannot be null");
        }
        final Optional<Use<?>> opt = expression.visit(this);
        return opt.map(v -> v.optional(false));
    }

    @Override
    public Optional<Use<?>> visitAdd(final Add expression) {

        return Optionals.map(visit(expression.getLhs()), visit(expression.getRhs()), (lhs, rhs) -> {
            if (lhs instanceof UseString || rhs instanceof UseString) {
                return UseString.DEFAULT;
            } else {
                return numericPromote(expression, lhs, rhs,
                        (l, r) -> DecimalContext.DEFAULT.addition(
                                l.getPrecision(), l.getScale(),
                                r.getPrecision(), r.getScale()
                        ));
            }
        });
    }

    @Override
    public Optional<Use<?>> visitDiv(final Div expression) {

        return Optionals.map(visit(expression.getLhs()), visit(expression.getRhs()),
                (lhs, rhs) -> numericPromote(expression, lhs, rhs,
                        (l, r) -> DecimalContext.DEFAULT.division(
                            l.getPrecision(), l.getScale(),
                            r.getPrecision(), r.getScale()
                    )));
    }

    @Override
    public Optional<Use<?>> visitMod(final Mod expression) {

        return Optionals.map(visit(expression.getLhs()), visit(expression.getRhs()),
                (lhs, rhs) -> numericPromote(expression, lhs, rhs, null));
    }

    @Override
    public Optional<Use<?>> visitMul(final Mul expression) {

        return Optionals.map(visit(expression.getLhs()), visit(expression.getRhs()),
                (lhs, rhs) -> numericPromote(expression, lhs, rhs,
                        (l, r) -> DecimalContext.DEFAULT.multiplication(
                            l.getPrecision(), l.getScale(),
                            r.getPrecision(), r.getScale()
                    )));
    }

    @Override
    public Optional<Use<?>> visitNegate(final Negate expression) {

        return visit(expression.getOperand());
    }

    @Override
    public Optional<Use<?>> visitPow(final Pow expression) {

        return Optionals.map(visit(expression.getLhs()), visit(expression.getRhs()),
                (lhs, rhs) -> numericPromote(expression, lhs, rhs, null));
    }

    @Override
    public Optional<Use<?>> visitSub(final Sub expression) {

        return Optionals.map(visit(expression.getLhs()), visit(expression.getRhs()),
                (lhs, rhs) -> numericPromote(expression, lhs, rhs,
                        (l, r) -> DecimalContext.DEFAULT.addition(
                                l.getPrecision(), l.getScale(),
                                r.getPrecision(), r.getScale()
                        )));
    }

    private Use<?> numericPromote(final Expression expression, final Use<?> lhs, final Use<?> rhs, final BiFunction<UseDecimal, UseDecimal, DecimalContext.PrecisionAndScale> decimalContext) {

        if(lhs instanceof UseDecimal || rhs instanceof UseDecimal) {
            if(decimalContext != null) {
                final UseDecimal lhsDecimal = lhs instanceof UseDecimal ? (UseDecimal) lhs : UseDecimal.DEFAULT;
                final UseDecimal rhsDecimal = rhs instanceof UseDecimal ? (UseDecimal) rhs : UseDecimal.DEFAULT;
                final DecimalContext.PrecisionAndScale ps = decimalContext.apply(lhsDecimal, rhsDecimal);
                return new UseDecimal(ps.getPrecision(), ps.getScale());
            } else {
                throw new UnsupportedOperationException("Cannot infer type of " + expression);
            }
        } else if(lhs instanceof UseNumber || rhs instanceof UseNumber) {
            return UseNumber.DEFAULT;
        } else {
            return UseInteger.DEFAULT;
        }
    }

    @Override
    public Optional<Use<?>> visitBitAnd(final BitAnd expression) {

        return Optional.of(UseInteger.DEFAULT);
    }

    @Override
    public Optional<Use<?>> visitBitFlip(final BitNot expression) {

        return Optional.of(UseInteger.DEFAULT);
    }

    @Override
    public Optional<Use<?>> visitBitLsh(final BitLsh expression) {

        return Optional.of(UseInteger.DEFAULT);
    }

    @Override
    public Optional<Use<?>> visitBitOr(final BitOr expression) {

        return Optional.of(UseInteger.DEFAULT);
    }

    @Override
    public Optional<Use<?>> visitBitRsh(final BitRsh expression) {

        return Optional.of(UseInteger.DEFAULT);
    }

    @Override
    public Optional<Use<?>> visitBitXor(final BitXor expression) {

        return Optional.of(UseInteger.DEFAULT);
    }

    @Override
    public Optional<Use<?>> visitCmp(final Cmp expression) {

        return Optional.of(UseInteger.DEFAULT);
    }

    @Override
    public Optional<Use<?>> visitEq(final Eq expression) {

        return Optional.of(UseBoolean.DEFAULT);
    }

    @Override
    public Optional<Use<?>> visitGt(final Gt expression) {

        return Optional.of(UseBoolean.DEFAULT);
    }

    @Override
    public Optional<Use<?>> visitGte(final Gte expression) {

        return Optional.of(UseBoolean.DEFAULT);
    }

    @Override
    public Optional<Use<?>> visitLt(final Lt expression) {

        return Optional.of(UseBoolean.DEFAULT);
    }

    @Override
    public Optional<Use<?>> visitLte(final Lte expression) {

        return Optional.of(UseBoolean.DEFAULT);
    }

    @Override
    public Optional<Use<?>> visitNe(final Ne expression) {

        return Optional.of(UseBoolean.DEFAULT);
    }

    @Override
    public Optional<Use<?>> visitConstant(final Constant expression) {

        final Object value = expression.getValue();
        return Optional.of(constantType(value));
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
    public Optional<Use<?>> visitNameConstant(final NameConstant expression) {

        final Name name = expression.getName();
        return context.optionalTypeOf(name);
    }

    @Override
    public Optional<Use<?>> visitCoalesce(final Coalesce expression) {

        return Optionals.map(visit(expression.getLhs()), visit(expression.getRhs()), Use::commonBase);
    }

    @Override
    public Optional<Use<?>> visitIfElse(final IfElse expression) {

        return Optionals.map(visit(expression.getThen()), visit(expression.getOtherwise()), Use::commonBase);
    }

    @Override
    public Optional<Use<?>> visitIn(final In expression) {

        return Optional.of(UseBoolean.DEFAULT);
    }

    @Override
    public Optional<Use<?>> visitIndex(final Index expression) {

        return visit(expression.getLhs()).map((lhs) -> {
            if (lhs instanceof UseCollection) {
                return ((UseCollection<?, ?>) lhs).getType();
            } else if (lhs instanceof UseMap<?>) {
                return ((UseMap<?>) lhs).getType();
            } else {
                throw new UnsupportedOperationException("Cannot infer type of " + expression);
            }
        });
    }

    @Override
    public Optional<Use<?>> visitLambda(final Lambda expression) {

        return Optional.empty();
    }

    @Override
    public Optional<Use<?>> visitLambdaCall(final LambdaCall expression) {

        final Expression with = expression.getWith();
        if(with instanceof NameConstant) {
            final List<Use<?>> args = expression.getArgs().stream()
                    .map(this::typeOf).collect(Collectors.toList());
            return Optional.of(typeOfCall(((NameConstant) with).getName(), args));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public Optional<Use<?>> visitMemberCall(final MemberCall expression) {

        final Use<?> with = typeOf(expression.getWith());
        final String member = expression.getMember();
        final List<Use<?>> args = expression.getArgs().stream()
                .map(this::typeOf).collect(Collectors.toList());
        return Optional.of(typeOfCall(with, member, args));
    }

    @Override
    public Optional<Use<?>> visitMember(final Member expression) {

        final Use<?> with = typeOf(expression.getWith());
        final String member = expression.getMember();
        if(with != null) {
            return Optional.of(with.typeOf(Name.of(member)));
        } else {
            throw new UnsupportedOperationException("Cannot infer type of " + expression);
        }
    }

    @Override
    public Optional<Use<?>> visitWith(final With expression) {

        final Map<String, Use<?>> with = expression.getWith().stream().collect(Collectors.toMap(
                Pair::getFirst,
                e -> typeOf(e.getSecond())
        ));
        final InferenceContext newContext = context.with(with);
        return new InferenceVisitor(newContext, methods).visit(expression.getYield());
    }

    @Override
    public Optional<Use<?>> visitForAll(final ForAll expression) {

        return Optional.of(UseBoolean.DEFAULT);
    }

    @Override
    public Optional<Use<?>> visitForAny(final ForAny expression) {

        return Optional.of(UseBoolean.DEFAULT);
    }
    
    private Map<String, Use<?>> iteratorTypes(final ContextIterator iterator) {
        
        if(iterator instanceof ContextIterator.OfKeyValue) {
            final ContextIterator.OfKeyValue ofKeyValue = (ContextIterator.OfKeyValue)iterator;
            final Use<?> containerType = typeOf(ofKeyValue.getExpr());
            if(containerType instanceof UseCollection) {
                return ImmutableMap.of(
                        ofKeyValue.getKey(), UseInteger.DEFAULT,
                        ofKeyValue.getValue(), ((UseCollection<?, ?>) containerType).getType()
                );
            } else if(containerType instanceof UseMap) {
                return ImmutableMap.of(
                        ofKeyValue.getKey(), UseString.DEFAULT,
                        ofKeyValue.getValue(), ((UseMap<?>) containerType).getType()
                );
            }
        } else if(iterator instanceof ContextIterator.OfValue) {
            final ContextIterator.OfValue ofValue = (ContextIterator.OfValue)iterator;
            final Use<?> containerType = typeOf(ofValue.getExpr());
            if(containerType instanceof UseCollection) {
                return ImmutableMap.of(
                        ofValue.getValue(), ((UseCollection<?, ?>) containerType).getType()
                );
            } else if(containerType instanceof UseMap) {
                return ImmutableMap.of(
                        ofValue.getValue(), ((UseMap<?>) containerType).getType()
                );
            }
        } else if(iterator instanceof ContextIterator.Where) {
            return iteratorTypes(((ContextIterator.Where) iterator).getIterator());
        }
        return ImmutableMap.of();
    }

    @Override
    public Optional<Use<?>> visitForArray(final ForArray expression) {

        final Expression yield = expression.getYield();
        final InferenceContext newContext = context.with(iteratorTypes(expression.getIterator()));
        final Use<?> valueType = new InferenceVisitor(newContext, methods).typeOf(yield);
        return Optional.of(UseArray.from(valueType));
    }

    @Override
    public Optional<Use<?>> visitForObject(final ForObject expression) {

        final Expression yield = expression.getYieldValue();
        final InferenceContext newContext = context.with(iteratorTypes(expression.getIterator()));
        final Use<?> valueType = new InferenceVisitor(newContext, methods).typeOf(yield);
        return Optional.of(UseMap.from(valueType));
    }

    @Override
    public Optional<Use<?>> visitForSet(final ForSet expression) {

        final Expression yield = expression.getYield();
        final InferenceContext newContext = context.with(iteratorTypes(expression.getIterator()));
        final Use<?> valueType = new InferenceVisitor(newContext, methods).typeOf(yield);
        return Optional.of(UseSet.from(valueType));
    }

    @Override
    public Optional<Use<?>> visitLiteralArray(final LiteralArray expression) {

        final Stream<Use<?>> args = expression.getArgs().stream().map(this::typeOf);
        return Optional.of(UseArray.from(args.reduce(Use::commonBase).orElse(UseAny.DEFAULT)));
    }

    @Override
    public Optional<Use<?>> visitLiteralObject(final LiteralObject expression) {

        final Stream<Use<?>> args = expression.getArgs().values().stream().map(this::typeOf);
        return Optional.of(UseMap.from(args.reduce(Use::commonBase).orElse(UseAny.DEFAULT)));
    }

    @Override
    public Optional<Use<?>> visitLiteralSet(final LiteralSet expression) {

        final Stream<Use<?>> args = expression.getArgs().stream().map(this::typeOf);
        return Optional.of(UseSet.from(args.reduce(Use::commonBase).orElse(UseAny.DEFAULT)));
    }

    @Override
    public Optional<Use<?>> visitAnd(final And expression) {

        return Optional.of(UseBoolean.DEFAULT);
    }

    @Override
    public Optional<Use<?>> visitNot(final Not expression) {

        return Optional.of(UseBoolean.DEFAULT);
    }

    @Override
    public Optional<Use<?>> visitOr(final Or expression) {

        return Optional.of(UseBoolean.DEFAULT);
    }

    @Override
    public Optional<Use<?>> visitLike(final Like like) {

        return Optional.of(UseBoolean.DEFAULT);
    }

    @Override
    public Optional<Use<?>> visitAvg(final Avg expression) {

        return Optional.of(UseNumber.DEFAULT);
    }

    @Override
    public Optional<Use<?>> visitCollectArray(final CollectArray expression) {

        return Optional.of(UseArray.from(typeOf(expression.getInput())));
    }

    @Override
    public Optional<Use<?>> visitCount(final Count expression) {

        return Optional.of(UseInteger.DEFAULT);
    }

    @Override
    public Optional<Use<?>> visitMax(final Max expression) {

        return visit(expression.getInput());
    }

    @Override
    public Optional<Use<?>> visitMin(final Min expression) {

        return visit(expression.getInput());
    }

    @Override
    public Optional<Use<?>> visitSum(final Sum expression) {

        return visit(expression.getInput());
    }

    @Override
    public Optional<Use<?>> visitSql(final Sql expression) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Use<?>> visitCast(final Cast expression) {

        return Optional.of(context.namedType(expression.getType()));
    }

    @Override
    public Optional<Use<?>> visitCase(final Case expression) {

        Stream<Expression> expressions = expression.getWhen().stream().map(Pair::getSecond);
        if(expression.getOtherwise() != null) {
            expressions = Stream.concat(expressions, Stream.of(expression.getOtherwise()));
        }

        final Optional<Optional<Use<?>>> result = expressions.map(this::optionalTypeOf)
                .reduce((a, b) -> Optionals.map(a, b, Use::commonBase));

        return result.orElseGet(Optional::empty);
    }

    protected Use<?> typeOfCall(final Use<?> target, final String member, final List<Use<?>> args) {

        final Type[] argTypes = args.stream().map(Use::javaType).toArray(Type[]::new);
        final Callable callable = methods.callable(target.javaType(), member, argTypes);
        return Use.fromJavaType(callable.type());
    }

    protected Use<?> typeOfCall(final Name name, final List<Use<?>> args) {

        final Type[] argTypes = args.stream().map(Use::javaType).toArray(Type[]::new);
        final Callable callable = methods.callable(name, argTypes);
        return Use.fromJavaType(callable.type());
    }
}
