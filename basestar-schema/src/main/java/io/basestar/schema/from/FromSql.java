package io.basestar.schema.from;

import io.basestar.expression.Expression;
import io.basestar.schema.Bucketing;
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.Schema;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.use.Use;
import io.basestar.util.BinaryKey;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * Temporary wrapper to keep hold of SQL expression for codegen
 */

@Data
@Deprecated
public class FromSql implements From {

    private final Expression sql;

    private final List<String> primaryKey;

    private final Map<String, From> using;

    private final From impl;

    @Override
    public Descriptor descriptor() {

        return new Descriptor.Defaulting() {

            @Override
            public Expression getSql() {

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

        return impl.inferenceContext();
    }

    @Override
    public void collectMaterializationDependencies(final Map<Name, LinkableSchema> out) {

        impl.collectMaterializationDependencies(out);
    }

    @Override
    public void collectDependencies(final Map<Name, Schema<?>> out) {

        impl.collectDependencies(out);
    }

    @Override
    public Expression id() {

        return impl.id();
    }

    @Override
    public Use<?> typeOfId() {

        return impl.typeOfId();
    }

    @Override
    public Map<String, Use<?>> getProperties() {

        return impl.getProperties();
    }

    @Override
    public BinaryKey id(final Map<String, Object> row) {

        return impl.id(row);
    }

    @Override
    public boolean isCompatibleBucketing(final List<Bucketing> other) {

        return impl.isCompatibleBucketing(other);
    }

    @Override
    public List<FromSchema> schemas() {

        return impl.schemas();
    }

    @Override
    public <T> T visit(final FromVisitor<T> visitor) {

        return impl.visit(visitor);
    }
}
