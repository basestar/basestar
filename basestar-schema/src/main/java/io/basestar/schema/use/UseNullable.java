package io.basestar.schema.use;

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

@Data
public class UseNullable<T> implements Use<T> {

    public static final String NAME = "nullable";

    private final Use<T> type;

    @Override
    public <R> R visit(final Visitor<R> visitor) {

        return visitor.visitNullable(this);
    }

    public static UseNullable<?> from(final Object config) {

        return Use.fromNestedConfig(config, (type, nestedConfig) -> new UseNullable<>(type));
    }

    @Override
    public boolean isNullable() {

        return true;
    }

    @Override
    public Use<T> nullable(final boolean nullable) {

        if(nullable) {
            return this;
        } else {
            return type.nullable(false);
        }
    }

    @Override
    public UseNullable<?> resolve(final Schema.Resolver resolver) {

        final Use<?> resolved = type.resolve(resolver);
        if(resolved == type) {
            return this;
        } else {
            return new UseNullable<>(resolved);
        }
    }

    @Override
    public T create(final Object value, final boolean expand, final boolean suppress) {

        return type.create(value, expand, suppress);
    }

    @Override
    public Code code() {

        return Code.NULLABLE;
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

        return null;
    }

    @Override
    public String toString() {

        return NAME + "<" + type + ">";
    }

    @Override
    public void serializeValue(final T value, final DataOutput out) throws IOException {

        type.serializeValue(value, out);
    }

    @Override
    public T deserializeValue(final DataInput in) throws IOException {

        return type.deserializeValue(in);
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
    public io.swagger.v3.oas.models.media.Schema<?> openApi() {

        return type.openApi();
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
