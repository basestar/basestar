package io.basestar.schema.from;

import com.google.common.collect.ImmutableMap;
import io.basestar.expression.Expression;
import io.basestar.expression.arithmetic.Add;
import io.basestar.schema.Bucketing;
import io.basestar.schema.Schema;
import io.basestar.schema.ViewSchema;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseBinary;
import io.basestar.util.BinaryKey;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Data
public class FromJoin implements From {

    private final Join join;

    public FromJoin(final Join join) {

        this.join = Nullsafe.require(join);
    }

//    protected FromJoin(final Schema.Resolver.Constructing resolver, final From.Descriptor from) {
//
//        super(from);
//        this.join = Nullsafe.require(from.getJoin()).build(resolver);
//    }

    @Override
    public From.Descriptor descriptor() {

        return new Descriptor.Defaulting() {
            @Override
            public Join.Descriptor getJoin() {

                return join.descriptor();
            }
        };
    }

    @Override
    public InferenceContext inferenceContext() {

        final From left = join.getLeft();
        final From right = join.getRight();

        final InferenceContext leftContext = left.inferenceContext();
        final InferenceContext rightContext = right.inferenceContext();

        InferenceContext result = new InferenceContext.Join(leftContext, rightContext);
        if(left.hasAlias()) {
            result = result.overlay(left.getAlias(), leftContext);
        }
        if(right.hasAlias()) {
            result = result.overlay(right.getAlias(), rightContext);
        }
        return result.with(ImmutableMap.of(
                ViewSchema.ID, UseBinary.DEFAULT
        ));
    }

    @Override
    public void collectMaterializationDependencies(final Map<Name, Schema> out) {

        join.collectMaterializationDependencies(out);
    }

    @Override
    public void collectDependencies(final Map<Name, Schema> out) {

        join.collectDependencies(out);
    }

    @Override
    public Use<?> typeOfId() {

        return UseBinary.DEFAULT;
    }

    @Override
    public Map<String, Use<?>> getProperties() {

        final Map<String, Use<?>> result = new HashMap<>();
        result.putAll(join.getLeft().getProperties());
        result.putAll(join.getRight().getProperties());
        return result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public BinaryKey id(final Map<String, Object> row) {

        final BinaryKey left = join.getLeft().id((Map<String, Object>)row.get(join.getLeft().getAlias()));
        final BinaryKey right = join.getRight().id((Map<String, Object>)row.get(join.getRight().getAlias()));
        return left.concat(right);
    }

    @Override
    public boolean isCompatibleBucketing(final List<Bucketing> other) {

        return join.getLeft().isCompatibleBucketing(other) && join.getRight().isCompatibleBucketing(other);
    }

    @Override
    public List<FromSchema> schemas() {

        return Stream.of(join.getLeft(), join.getRight()).flatMap(from -> from.schemas().stream())
                .collect(Collectors.toList());
    }

    @Override
    public Expression id() {

        return new Add(join.getLeft().id(), join.getRight().id());
    }

    @Override
    public <T> T visit(final FromVisitor<T> visitor) {

        return visitor.visitJoin(this);
    }

    @Override
    public boolean isExternal() {

        return join.getLeft().isExternal() || join.getRight().isExternal();
    }
}
