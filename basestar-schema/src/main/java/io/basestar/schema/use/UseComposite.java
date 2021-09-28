package io.basestar.schema.use;

import com.google.common.collect.ImmutableMap;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.Bucketing;
import io.basestar.schema.Constraint;
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.Schema;
import io.basestar.schema.exception.TypeSyntaxException;
import io.basestar.schema.util.Cascade;
import io.basestar.schema.util.Expander;
import io.basestar.schema.util.Ref;
import io.basestar.schema.util.ValueContext;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.leangen.geantyref.TypeFactory;
import lombok.Data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;
import java.util.stream.Collectors;

@Data
public class UseComposite implements Use<Map<String, Object>> {

    public static final String NAME = "composite";

    private final SortedMap<String, Use<?>> types;

    public UseComposite(final Map<String, Use<?>> types) {

        this.types = Immutable.sortedMap(types);
    }

    public static UseComposite from(final Object config) {

        if (config instanceof Map) {
            final Map<String, Use<?>> types = new HashMap<>();
            ((Map<?, ?>) config).forEach((k, v) -> types.put((String) k, Use.fromNestedConfig(v)));
            return new UseComposite(types);
        } else {
            throw new TypeSyntaxException();
        }
    }

    @Override
    public <R> R visit(final Visitor<R> visitor) {

        return visitor.visitComposite(this);
    }

    @Override
    public Use<?> resolve(final Schema.Resolver resolver) {

        final Map<String, Use<?>> newTypes = new HashMap<>();
        boolean changed = false;
        for (final Map.Entry<String, Use<?>> entry : types.entrySet()) {
            final Use<?> type = entry.getValue();
            final Use<?> resolved = type.resolve(resolver);
            if (resolved != type) {
                changed = true;
            }
            newTypes.put(entry.getKey(), resolved);
        }
        if (changed) {
            return new UseComposite(newTypes);
        } else {
            return this;
        }
    }

    @Override
    public Map<String, Object> create(final ValueContext context, final Object value, final Set<Name> expand) {

        return context.createComposite(this, value, expand);
    }

    @Override
    public Code code() {

        return Code.COMPOSITE;
    }

    @Override
    public Optional<Use<?>> optionalTypeOf(final Name name) {

        if (name.isEmpty()) {
            return Optional.of(this);
        } else {
            final String first = name.first();
            final Use<?> type = types.get(first);
            if (type != null) {
                return type.optionalTypeOf(name.withoutFirst());
            } else {
                return Optional.empty();
            }
        }
    }

    @Override
    public Type javaType(final Name name) {

        if (name.isEmpty()) {
            return TypeFactory.parameterizedClass(Map.class, String.class, Object.class);
        } else {
            final String first = name.first();
            final Use<?> type = types.get(first);
            if (type != null) {
                return type.javaType(name.withoutFirst());
            } else {
                throw new IllegalStateException("Composite has no member " + first);
            }
        }
    }

    @Override
    public Map<String, Object> expand(final Name parent, final Map<String, Object> value, final Expander expander, final Set<Name> expand) {

        final Map<String, Set<Name>> branches = Name.branch(expand);
        return transformTypedValues(types, value, new TypedValueFunction() {
            @Override
            public <T> T apply(final String key, final Use<T> type, final T before) {

                if (branches.containsKey(key)) {
                    return type.expand(parent.with(key), before, expander, branches.get(key));
                } else {
                    return before;
                }
            }
        });
    }

    @Override
    public void expand(final Name parent, final Expander expander, final Set<Name> expand) {

    }

    @Override
    public Set<Name> requiredExpand(final Set<Name> names) {

        final Set<Name> result = new HashSet<>();
        Name.branch(names)
                .forEach((head, tail) -> types.get(head).requiredExpand(tail)
                        .forEach(path -> result.add(Name.of(head).with(path))));
        return result;
    }

    @Override
    public Map<String, Object> defaultValue() {

        return Immutable.transformValues(types, (key, value) -> value.defaultValue());
    }

    @Override
    public Object toConfig(final boolean optional) {

        return ImmutableMap.of(
                Use.name(NAME, optional), Immutable.transformValues(types, (k, v) -> v.toConfig())
        );
    }

    @Override
    public String toString(final Map<String, Object> value) {

        if (value == null) {
            return "null";
        } else {
            return "{" + types.entrySet().stream().sorted(Map.Entry.comparingByKey())
                    .map(v -> {
                        @SuppressWarnings("unchecked") final Use<Object> type = (Use<Object>) v.getValue();
                        return v.getKey() + ": " + type.toString(value.get(v.getKey()));
                    })
                    .collect(Collectors.joining(", ")) + "}";
        }
    }

    @Override
    public void serializeValue(final Map<String, Object> value, final DataOutput out) throws IOException {

        out.writeInt(types.size());
        for (final Map.Entry<String, Use<?>> entry : types.entrySet()) {
            UseString.DEFAULT.serializeValue(entry.getKey(), out);
            @SuppressWarnings("unchecked") final Use<Object> type = (Use<Object>) entry.getValue();
            type.serialize(value.get(entry.getKey()), out);
        }
    }

    @Override
    public Map<String, Object> deserializeValue(final DataInput in) throws IOException {

        return create(deserializeAnyValue(in));
    }

    public static <T> Map<String, T> deserializeAnyValue(final DataInput in) throws IOException {

        final Map<String, T> result = new HashMap<>();
        final int size = in.readInt();
        for (int i = 0; i != size; ++i) {
            final String key = UseString.DEFAULT.deserializeValue(in);
            final T value = Use.deserializeAny(in);
            result.put(key, value);
        }
        return result;
    }

    @Override
    public Map<String, Object> applyVisibility(final Context context, final Map<String, Object> value) {

        return transformTypedValues(types, value, new TypedValueFunction() {
            @Override
            public <T> T apply(final String key, final Use<T> type, final T before) {

                return type.applyVisibility(context, before);
            }
        });
    }

    @Override
    public Map<String, Object> evaluateTransients(final Context context, final Map<String, Object> value, final Set<Name> expand) {

        final Map<String, Set<Name>> branches = Name.branch(expand);
        return transformTypedValues(types, value, new TypedValueFunction() {
            @Override
            public <T> T apply(final String key, final Use<T> type, final T before) {

                final Set<Name> branch = branches.get(key);
                if (branch != null) {
                    return type.evaluateTransients(context, before, branch);
                } else {
                    return before;
                }
            }
        });
    }

    @Override
    public Set<Name> transientExpand(final Name name, final Set<Name> expand) {

        final Map<String, Set<Name>> branch = Name.branch(expand);
        final Set<Name> result = new HashSet<>(expand);
        branch.forEach((k, v) -> {
            final Use<?> type = types.get(k);
            if (type != null) {
                result.addAll(type.transientExpand(name.with(k), v));
            }
        });
        return result;
    }

    @Override
    public Set<Constraint.Violation> validateType(final Context context, final Name name, final Map<String, Object> value) {
        return types.entrySet().stream()
                .flatMap(e -> {
                    @SuppressWarnings("unchecked") final Use<Object> type = (Use<Object>) e.getValue();
                    final Object v = value.get(e.getKey());
                    return type.validate(context, name.with(e.getKey()), v).stream();
                })
                .collect(Collectors.toSet());
    }

    @Override
    public io.swagger.v3.oas.models.media.Schema<?> openApi(final Set<Name> expand) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Expression> refQueries(final Name otherSchemaName, final Set<Name> expand, final Name name) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Expression> cascadeQueries(final Cascade cascade, final Name otherSchemaName, final Name name) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Name> refExpand(final Name otherSchemaName, final Set<Name> expand) {

        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Ref, Long> refVersions(final Map<String, Object> value) {

        throw new UnsupportedOperationException();
    }

    @Override
    public void collectDependencies(final Set<Name> expand, final Map<Name, Schema<?>> out) {

        final Map<String, Set<Name>> branches = Name.branch(expand);
        for (final Map.Entry<String, Use<?>> entry : types.entrySet()) {
            final Set<Name> branch = branches.get(entry.getKey());
            if (branch != null) {
                entry.getValue().collectDependencies(branch, out);
            }
        }
    }

    @Override
    public void collectMaterializationDependencies(final Set<Name> expand, final Map<Name, LinkableSchema> out) {

        final Map<String, Set<Name>> branches = Name.branch(expand);
        for (final Map.Entry<String, Use<?>> entry : types.entrySet()) {
            final Set<Name> branch = branches.get(entry.getKey());
            if (branch != null) {
                entry.getValue().collectMaterializationDependencies(branch, out);
            }
        }
    }

    @Override
    public boolean isCompatibleBucketing(final List<Bucketing> other, final Name name) {

        if (name.isEmpty()) {
            return false;
        } else {
            final Use<?> type = types.get(name.first());
            if (type != null) {
                return type.isCompatibleBucketing(other, name.withoutFirst());
            } else {
                return false;
            }
        }
    }

    @Override
    public boolean areEqual(final Map<String, Object> a, final Map<String, Object> b) {

        if (a == null || b == null) {
            return a == null && b == null;
        } else if (a.size() == b.size()) {
            for (final Map.Entry<String, Use<?>> entry : types.entrySet()) {
                final String key = entry.getKey();
                @SuppressWarnings("unchecked") final Use<Object> type = (Use<Object>) entry.getValue();
                if (!type.areEqual(a.get(key), b.get(key))) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    private interface TypedValueFunction {

        <T> T apply(String key, Use<T> type, T before);
    }

    private static Map<String, Object> transformTypedValues(final Map<String, Use<?>> types, final Map<String, Object> value, final TypedValueFunction fn) {

        if (value != null) {
            final Map<String, Object> changed = new HashMap<>();
            for (final Map.Entry<String, Use<?>> entry : types.entrySet()) {
                final String key = entry.getKey();
                @SuppressWarnings("unchecked") final Use<Object> type = (Use<Object>) entry.getValue();
                final Object before = value.get(key);
                final Object after = fn.apply(key, type, before);
                if (before != after) {
                    changed.put(key, after);
                }
            }
            if (changed.isEmpty()) {
                return value;
            } else {
                final Map<String, Object> copy = new HashMap<>(value);
                copy.putAll(changed);
                return copy;
            }
        } else {
            return null;
        }
    }
}
