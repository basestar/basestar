package io.basestar.schema.from;

import io.basestar.expression.Expression;
import io.basestar.schema.Bucketing;
import io.basestar.schema.Schema;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.use.Use;
import io.basestar.util.BinaryKey;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class FromAlias implements From {

    private final From from;

    private final String alias;

    public FromAlias(final From from, final String as) {

        this.from = Nullsafe.require(from);
        this.alias = Nullsafe.require(as);
    }

    @Override
    public Descriptor descriptor() {

        final Descriptor descriptor = from.descriptor();
        return new Descriptor.Delegating() {

            @Override
            public Descriptor delegate() {

                return descriptor;
            }

            @Override
            public String getAs() {

                return alias;
            }
        };
    }

    @Override
    public InferenceContext inferenceContext() {

        final InferenceContext context = from.inferenceContext();
        return context.overlay(alias, context);
    }

    @Override
    public void collectMaterializationDependencies(final Map<Name, Schema> out) {

        from.collectMaterializationDependencies(out);
    }

    @Override
    public void collectDependencies(final Map<Name, Schema> out) {

        from.collectDependencies(out);
    }

    @Override
    public Expression id() {

        return from.id();
    }

    @Override
    public Use<?> typeOfId() {

        return from.typeOfId();
    }

    @Override
    public Map<String, Use<?>> getProperties() {

        return from.getProperties();
    }

    @Override
    public BinaryKey id(final Map<String, Object> row) {

        return from.id(row);
    }

    @Override
    public boolean isCompatibleBucketing(final List<Bucketing> other) {

        return from.isCompatibleBucketing(other);
    }

    @Override
    public List<FromSchema> schemas() {

        return from.schemas();
    }

    @Override
    public <T> T visit(final FromVisitor<T> visitor) {

        return visitor.visitAlias(this);
    }

    @Override
    public boolean hasAlias() {

        return true;
    }

    @Override
    public boolean isExternal() {

        return from.isExternal();
    }
}
