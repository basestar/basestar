package io.basestar.schema.from;

import io.basestar.expression.Expression;
import io.basestar.schema.Bucketing;
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.Schema;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.use.Use;
import io.basestar.util.*;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class FromSort implements From {

    private final From from;

    private final List<Sort> sort;

    public FromSort(final From from, final List<Sort> sort) {

        this.from = Nullsafe.require(from);
        this.sort = Immutable.list(sort);
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
            public List<Sort> getOrder() {

                return sort;
            }
        };
    }

    @Override
    public InferenceContext inferenceContext() {

        return from.inferenceContext();
    }

    @Override
    public void collectMaterializationDependencies(final Map<Name, LinkableSchema> out) {

        from.collectMaterializationDependencies(out);
    }

    @Override
    public void collectDependencies(final Map<Name, Schema<?>> out) {

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

        return visitor.visitSort(this);
    }
}
