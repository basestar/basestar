//package io.basestar.schema.use;
//
//import io.basestar.expression.Context;
//import io.basestar.expression.Expression;
//import io.basestar.expression.call.Callable;
//import io.basestar.schema.Bucketing;
//import io.basestar.schema.Constraint;
//import io.basestar.schema.LinkableSchema;
//import io.basestar.schema.Schema;
//import io.basestar.schema.util.Cascade;
//import io.basestar.schema.util.Expander;
//import io.basestar.schema.util.Ref;
//import io.basestar.schema.util.ValueContext;
//import io.basestar.util.Name;
//
//import java.io.DataInput;
//import java.io.DataOutput;
//import java.io.IOException;
//import java.lang.reflect.Type;
//import java.util.*;
//
//public class UseCallable implements Use<Callable> {
//
//    public static final String NAME = "callable";
//
//    private final Use<?> result;
//
//    private final List<Use<?>> args;
//
//    @Override
//    public <R> R visit(final Use.Visitor<R> visitor) {
//
//        return visitor.visitCallable(this);
//    }
//
//    @Override
//    public Object toConfig(final boolean optional) {
//
//        return Use.name(NAME, optional);
//    }
//
//    @Override
//    public UseCallable resolve(final Schema.Resolver resolver) {
//
//        final Use<?> result = this.result.resolve(resolver);
//
//
//        return this;
//    }
//
//    @Override
//    public Object create(final ValueContext context, final Object value, final Set<Name> expand) {
//
//        return context.createAny(this, value, expand);
//    }
//
//    @Override
//    public Use.Code code() {
//
//        return Use.Code.ANY;
//    }
//
//    @Override
//    public io.swagger.v3.oas.models.media.Schema<?> openApi(final Set<Name> expand) {
//
//        return new io.swagger.v3.oas.models.media.Schema<>();
//    }
//
//    @Override
//    public void serializeValue(final Object value, final DataOutput out) throws IOException {
//
//        //FIXME:
//    }
//
//    @Override
//    public Object deserializeValue(final DataInput in) throws IOException {
//
//        return deserializeAnyValue(in);
//    }
//
//    public static Object deserializeAnyValue(final DataInput in) throws IOException {
//
//        //FIXME:
//        return null;
//    }
//
//    @Override
//    public Set<Expression> refQueries(final Name otherSchemaName, final Set<Name> expand, final Name name) {
//
//        return Collections.emptySet();
//    }
//
//    @Override
//    public Set<Expression> cascadeQueries(final Cascade cascade, final Name otherSchemaName, final Name name) {
//
//        return Collections.emptySet();
//    }
//
//    @Override
//    public Set<Name> refExpand(final Name otherSchemaName, final Set<Name> expand) {
//
//        return Collections.emptySet();
//    }
//
//    @Override
//    public Map<Ref, Long> refVersions(final Object value) {
//
//        return Collections.emptyMap();
//    }
//
//    @Override
//    public Optional<Use<?>> optionalTypeOf(final Name name) {
//
//        return Optional.of(this);
//    }
//
//    @Override
//    public Type javaType(final Name name) {
//
//        return Object.class;
//    }
//
//    private static Set<Name> branch(final Map<String, Set<Name>> branches, final String key) {
//
//        return Collections.emptySet();
//    }
//
//    @Override
//    public Object expand(final Name parent, final Object value, final Expander expander, final Set<Name> expand) {
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
//    public Object applyVisibility(final Context context, final Object value) {
//
//        return value;
//    }
//
//    @Override
//    public Object evaluateTransients(final Context context, final Object value, final Set<Name> expand) {
//
//        return value;
//    }
//
//    @Override
//    public Set<Name> transientExpand(final Name name, final Set<Name> expand) {
//
//        return Collections.emptySet();
//    }
//
//    @Override
//    public Set<Constraint.Violation> validate(final Context context, final Name name, final Object value) {
//
//        return Collections.emptySet();
//    }
//
//    @Override
//    public Set<Name> requiredExpand(final Set<Name> names) {
//
//        return Collections.emptySet();
//    }
//
//    @Override
//    public Object defaultValue() {
//
//        return null;
//    }
//
//    @Override
//    public String toString() {
//
//        return NAME;
//    }
//
//    @Override
//    public String toString(final Object value) {
//
//        return Objects.toString(value);
//    }
//
//    @Override
//    public void collectDependencies(final Set<Name> expand, final Map<Name, Schema<?>> out) {
//
//    }
//
//    @Override
//    public void collectMaterializationDependencies(final Set<Name> expand, final Map<Name, LinkableSchema> out) {
//
//    }
//
//    @Override
//    public boolean isCompatibleBucketing(final List<Bucketing> other, final Name name) {
//
//        return false;
//    }
//
//    @Override
//    public boolean areEqual(final Object a, final Object b) {
//
//        return Objects.equals(a, b);
//    }
//}
