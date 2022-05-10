package io.basestar.schema.from;

import io.basestar.expression.Expression;
import io.basestar.schema.Bucketing;
import io.basestar.schema.CallableSchema;
import io.basestar.schema.Schema;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseBinary;
import io.basestar.util.BinaryKey;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import lombok.Data;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
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

    public String getReplacedSql(final BiFunction<Schema, Boolean, String> replacer) {

        return CallableSchema.getReplacedDefinition(sql, using, replacer);
    }
}
