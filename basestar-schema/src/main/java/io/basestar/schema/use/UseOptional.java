package io.basestar.schema.use;

import com.google.common.collect.ImmutableMap;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.Constraint;
import io.basestar.schema.Schema;
import io.basestar.schema.util.Expander;
import io.basestar.schema.util.Ref;
import io.basestar.util.Name;
import lombok.Data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

@Data
public class UseOptional<T> implements Use<T> {

    public static final String SYMBOL = "?";

    public static final String NAME = "optional";

    private final Use<T> type;

    @Override
    public <R> R visit(final Visitor<R> visitor) {

        return visitor.visitOptional(this);
    }

    public UseOptional<?> transform(final Function<Use<T>, Use<?>> fn) {

        final Use<?> type2 = fn.apply(type);
        if(type2 == type ) {
            return this;
        } else {
            return new UseOptional<>(type2);
        }
    }

    public static UseOptional<?> from(final Object config) {

        return Use.fromNestedConfig(config, (type, nestedConfig) -> new UseOptional<>(type));
    }

    @Override
    public boolean isOptional() {

        return true;
    }

    @Override
    public Use<T> optional(final boolean nullable) {

        if(nullable) {
            return this;
        } else {
            return type.optional(false);
        }
    }

    @Override
    public UseOptional<?> resolve(final Schema.Resolver resolver) {

        final Use<?> resolved = type.resolve(resolver);
        if(resolved == type) {
            return this;
        } else {
            return new UseOptional<>(resolved);
        }
    }

    @Override
    public T create(final Object value, final Set<Name> expand, final boolean suppress) {

        if(value == null) {
            return null;
        } else {
            return type.create(value, expand, suppress);
        }
    }

    @Override
    public Code code() {

        return type.code();
    }

    @Override
    public Use<?> typeOf(final Name name) {

        return type.typeOf(name);
    }

    @Override
    public T expand(final T value, final Expander expander, final Set<Name> expand) {

        return type.expand(value, expander, expand);
    }

    @Override
    public Set<Name> requiredExpand(final Set<Name> names) {

        return type.requiredExpand(names);
    }

    @Override
    public Object toConfig() {

        return ImmutableMap.of(
                NAME, type
        );
    }

    @Override
    public String toString() {

        return NAME + "<" + type + ">";
    }

    @Override
    public String toString(final T value) {

        return value == null ? "null" : type.toString(value);
    }

    @Override
    public void serialize(final T value, final DataOutput out) throws IOException {

        // Skip emitting a wrapper for optional, since we allow nulls anywhere
        serializeValue(value, out);
    }

    @Override
    public T deserialize(final DataInput in) throws IOException {

        // Skip emitting a wrapper for optional, since we allow nulls anywhere
        return deserializeValue(in);
    }

    @Override
    public void serializeValue(final T value, final DataOutput out) throws IOException {

        type.serialize(value, out);
    }

    @Override
    public T deserializeValue(final DataInput in) throws IOException {

        return type.deserialize(in);
    }

    @Override
    public T applyVisibility(final Context context, final T value) {

        return type.applyVisibility(context, value);
    }

    @Override
    public T evaluateTransients(final Context context, final T value, final Set<Name> expand) {

        return type.evaluateTransients(context, value, expand);
    }

    @Override
    public Set<Name> transientExpand(final Name name, final Set<Name> expand) {

        return type.transientExpand(name, expand);
    }

    @Override
    public Set<Constraint.Violation> validate(final Context context, final Name name, final T value) {

        return type.validate(context, name, value);
    }

    @Override
    public io.swagger.v3.oas.models.media.Schema<?> openApi(final Set<Name> expand) {

        return type.openApi(expand);
    }

    @Override
    public Set<Expression> refQueries(final Name otherSchemaName, final Set<Name> expand, final Name name) {

        return type.refQueries(otherSchemaName, expand, name);
    }

    @Override
    public Set<Name> refExpand(final Name otherSchemaName, final Set<Name> expand) {

        return type.refExpand(otherSchemaName, expand);
    }

    @Override
    public Map<Ref, Long> refVersions(final T value) {

        return type.refVersions(value);
    }

    @Override
    public void collectDependencies(final Set<Name> expand, final Map<Name, Schema<?>> out) {

        type.collectDependencies(expand, out);
    }
}
