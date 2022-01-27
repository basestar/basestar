package io.basestar.schema.from;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.basestar.expression.Expression;
import io.basestar.expression.constant.Constant;
import io.basestar.schema.Bucketing;
import io.basestar.schema.Schema;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseBinary;
import io.basestar.util.BinaryKey;
import io.basestar.util.Bytes;
import io.basestar.util.Name;

import java.util.List;
import java.util.Map;

public class FromExternal implements From {

    @Override
    public Descriptor descriptor() {

        return new From.Descriptor.Defaulting() {
        };
    }

    @Override
    public InferenceContext inferenceContext() {

        return InferenceContext.empty();
    }

    @Override
    public void collectMaterializationDependencies(final Map<Name, Schema> out) {

    }

    @Override
    public void collectDependencies(final Map<Name, Schema> out) {

    }

    @Override
    public Expression id() {

        return new Constant(Bytes.empty());
    }

    @Override
    public Use<?> typeOfId() {

        return UseBinary.DEFAULT;
    }

    @Override
    public Map<String, Use<?>> getProperties() {

        return ImmutableMap.of();
    }

    @Override
    public BinaryKey id(final Map<String, Object> row) {

        return BinaryKey.empty();
    }

    @Override
    public boolean isCompatibleBucketing(final List<Bucketing> other) {

        return false;
    }

    @Override
    public List<FromSchema> schemas() {

        return ImmutableList.of();
    }

    @Override
    public <T> T visit(final FromVisitor<T> visitor) {

        return visitor.visitExternal(this);
    }

    @Override
    public boolean isExternal() {

        return true;
    }
}
