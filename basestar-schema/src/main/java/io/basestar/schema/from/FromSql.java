package io.basestar.schema.from;

import io.basestar.expression.Expression;
import io.basestar.schema.Argument;
import io.basestar.schema.Bucketing;
import io.basestar.schema.CallableSchema;
import io.basestar.schema.Schema;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseBinary;
import io.basestar.util.BinaryKey;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Pair;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Temporary wrapper to keep hold of SQL expression for codegen
 */

@Data
@Deprecated
public class FromSql implements From {

    private final String sql;

    private final List<String> primaryKey;

    private final Map<String, From> using;

    private final From impl;

    @Override
    public Descriptor descriptor() {

        return new Descriptor.Defaulting() {

            @Override
            public String getSql() {

                return sql;
            }

            @Override
            public List<String> getPrimaryKey() {

                return primaryKey;
            }

            @Override
            public Map<String, From.Descriptor> getUsing() {

                return Immutable.transformValues(using, (k, v) -> v.descriptor());
            }
        };
    }

    @Override
    public InferenceContext inferenceContext() {

        if (impl != null) {
            return impl.inferenceContext();
        } else {
            return InferenceContext.empty();
        }
    }

    @Override
    public void collectMaterializationDependencies(final Map<Name, Schema> out) {

        if (impl != null) {
            impl.collectMaterializationDependencies(out);
        }
        using.forEach((k, v) -> v.collectMaterializationDependencies(out));

    }

    @Override
    public void collectDependencies(final Map<Name, Schema> out) {

        if (impl != null) {
            impl.collectDependencies(out);
        }
        using.forEach((k, v) -> v.collectDependencies(out));
    }

    @Override
    public Expression id() {

        if (impl != null) {
            return impl.id();
        } else {
            throw new UnsupportedOperationException("Raw SQL view cannot be processed");
        }
    }

    @Override
    public Use<?> typeOfId() {

        if (impl != null) {
            return impl.typeOfId();
        } else {
            return UseBinary.DEFAULT;
        }
    }

    @Override
    public Map<String, Use<?>> getProperties() {

        if (impl != null) {
            return impl.getProperties();
        } else {
            throw new UnsupportedOperationException("Raw SQL view cannot be processed");
        }
    }

    @Override
    public BinaryKey id(final Map<String, Object> row) {

        if (impl != null) {
            return impl.id(row);
        } else {
            throw new UnsupportedOperationException("Raw SQL view cannot be processed");
        }
    }

    @Override
    public boolean isCompatibleBucketing(final List<Bucketing> other) {

        if (impl != null) {
            return impl.isCompatibleBucketing(other);
        } else {
            return false;
        }
    }

    @Override
    public List<FromSchema> schemas() {

        return Stream.concat(
                getUsing().values().stream().flatMap(from -> from.schemas().stream()),
                impl.schemas().stream()
        ).collect(Collectors.toList());
    }

    @Override
    public <T> T visit(final FromVisitor<T> visitor) {

        if (impl != null) {
            return impl.visit(visitor);
        } else {
            throw new UnsupportedOperationException("Raw SQL view cannot be processed");
        }
    }

    @Override
    public boolean isExternal() {

        return impl == null;
    }

    public String getReplacedSql(final Function<Schema, String> replacer) {

        return CallableSchema.getReplacedDefinition(sql, using, replacer);
    }

    public Pair<String, List<Object>> getReplacedSqlWithBindings(final Function<Schema, String> replacer, final List<Argument> arguments, final Map<String, Object> values) {

        final String sql = getReplacedSql(replacer);

        final List<Object> bindings = new ArrayList<>();
        final StringBuffer str = new StringBuffer();

        final Pattern pattern = Pattern.compile("\\$\\{(.*?)}");
        final Matcher matcher = pattern.matcher(sql);
        while (matcher.find()) {
            final String name = matcher.group(1);
            final Argument argument = arguments.stream().filter(arg -> name.equals(arg.getName()))
                    .findFirst().orElseThrow(() -> new IllegalStateException("Argument " + name + " not found"));
            final Object value = argument.getType().create(values.get(argument.getName()));
            matcher.appendReplacement(str, "?");
            bindings.add(value);
        }
        matcher.appendTail(str);

        return Pair.of(str.toString(), bindings);
    }
}
