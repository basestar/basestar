//package io.basestar.schema.use;
//
//import com.google.common.collect.ImmutableMap;
//import com.google.common.collect.ImmutableSet;
//import com.google.common.collect.Streams;
//import io.basestar.expression.Context;
//import io.basestar.expression.Expression;
//import io.basestar.expression.exception.TypeConversionException;
//import io.basestar.schema.Constraint;
//import io.basestar.schema.Schema;
//import io.basestar.schema.util.Expander;
//import io.basestar.schema.util.Ref;
//import io.basestar.schema.util.ValueContext;
//import io.basestar.util.Name;
//import io.leangen.geantyref.TypeFactory;
//
//import java.io.DataInput;
//import java.io.DataOutput;
//import java.io.IOException;
//import java.lang.reflect.Type;
//import java.util.List;
//import java.util.Map;
//import java.util.Objects;
//import java.util.Set;
//import java.util.function.Function;
//import java.util.stream.Collectors;
//
//public class UseFunction<T> implements Use<Function<Object[], T>> {
//
//    public static final String NAME = "function";
//
//    private final Use<T> ret;
//
//    private final List<Use<?>> args;
//
//    public UseFunction(final Use<T> ret, final List<Use<?>> args) {
//
//        this.ret = ret;
//        this.args = args;
//    }
//
//    @Override
//    public <R> R visit(final Visitor<R> visitor) {
//
//        return visitor.visitFunction(this);
//    }
//
//    @Override
//    @SuppressWarnings("UnstableApiUsage")
//    public Use<?> resolve(final Schema.Resolver resolver) {
//
//        final Use<?> resolvedRet = ret.resolve(resolver);
//        final List<Use<?>> resolvedArgs = args.stream().map(v -> v.resolve(resolver)).collect(Collectors.toList());
//        if(resolvedRet == ret && Streams.zip(resolvedArgs.stream(), args.stream(), (a, b) -> a == b).allMatch(v -> v)) {
//            return this;
//        } else {
//            return new UseFunction<>(resolvedRet, resolvedArgs);
//        }
//    }
//
//    @Override
//    @SuppressWarnings("unchecked")
//    public Function<Object[], T> create(final ValueContext context, final Object value, final Set<Name> expand) {
//
//        if(value instanceof Function) {
//            return (Function<Object[], T>)value;
//        } else {
//            throw new TypeConversionException(Function.class, value);
//        }
//    }
//
//    @Override
//    public Code code() {
//
//        // ser/deser is not supported
//        throw new UnsupportedOperationException();
//    }
//
//    @Override
//    public Use<?> typeOf(final Name name) {
//
//        if(name.isEmpty()) {
//            return this;
//        } else {
//            throw new IllegalStateException();
//        }
//    }
//
//    @Override
//    public Type javaType(final Name name) {
//
//        if(name.isEmpty()) {
//            return TypeFactory.parameterizedClass(Function.class, Object[].class, ret.javaType());
//        } else {
//            throw new IllegalStateException();
//        }
//    }
//
//    @Override
//    public Function<Object[], T> expand(final Name parent, final Function<Object[], T> value, final Expander expander, final Set<Name> expand) {
//
//        return value;
//    }
//
//    @Override
//    public void expand(final Name parent, final Expander expander, final Set<Name> expand) {
//
//    }
//
//    @Override
//    public Set<Name> requiredExpand(final Set<Name> names) {
//
//        return ImmutableSet.of();
//    }
//
//    @Override
//    public Function<Object[], T> defaultValue() {
//
//        return args -> null;
//    }
//
//    @Override
//    public Object toConfig(final boolean optional) {
//
//        throw new UnsupportedOperationException();
//    }
//
//    @Override
//    public String toString(final Function<Object[], T> value) {
//
//        return Objects.toString(value);
//    }
//
//    @Override
//    public void serializeValue(final Function<Object[], T> value, final DataOutput out) throws IOException {
//
//        throw new UnsupportedOperationException();
//    }
//
//    @Override
//    public Function<Object[], T> deserializeValue(final DataInput in) throws IOException {
//
//        throw new UnsupportedOperationException();
//    }
//
//    @Override
//    public Function<Object[], T> applyVisibility(final Context context, final Function<Object[], T> value) {
//
//        return value;
//    }
//
//    @Override
//    public Function<Object[], T> evaluateTransients(final Context context, final Function<Object[], T> value, final Set<Name> expand) {
//
//        return value;
//    }
//
//    @Override
//    public Set<Name> transientExpand(final Name name, final Set<Name> expand) {
//
//        return ImmutableSet.of();
//    }
//
//    @Override
//    public Set<Constraint.Violation> validate(final Context context, final Name name, final Function<Object[], T> value) {
//
//        return ImmutableSet.of();
//    }
//
//    @Override
//    public io.swagger.v3.oas.models.media.Schema<?> openApi(final Set<Name> expand) {
//
//        throw new UnsupportedOperationException();
//    }
//
//    @Override
//    public Set<Expression> refQueries(final Name otherSchemaName, final Set<Name> expand, final Name name) {
//
//        return ImmutableSet.of();
//    }
//
//    @Override
//    public Set<Name> refExpand(final Name otherSchemaName, final Set<Name> expand) {
//
//        return ImmutableSet.of();
//    }
//
//    @Override
//    public Map<Ref, Long> refVersions(final Function<Object[], T> value) {
//
//        return ImmutableMap.of();
//    }
//
//    @Override
//    public void collectDependencies(final Set<Name> expand, final Map<Name, Schema> out) {
//
//    }
//
//    @Override
//    public boolean areEqual(final Function<Object[], T> a, final Function<Object[], T> b) {
//
//        return Objects.equals(a, b);
//    }
//}
