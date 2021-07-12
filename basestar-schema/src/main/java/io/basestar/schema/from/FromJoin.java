package io.basestar.schema.from;

import com.google.common.collect.ImmutableMap;
import io.basestar.expression.Expression;
import io.basestar.expression.arithmetic.Add;
import io.basestar.schema.Bucketing;
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.Schema;
import io.basestar.schema.ViewSchema;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseBinary;
import io.basestar.schema.use.UseString;
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
        // Temp exceptions, should support anon left/right sides as long as there are no naming conflicts
        if(!left.hasAlias()) {
           throw new IllegalStateException("Left side of join must be named (with as=)");
        }
        if(!right.hasAlias()) {
            throw new IllegalStateException("Right side of join must be named (with as=)");
        }

        return InferenceContext.from(ImmutableMap.of(ViewSchema.ID, UseString.DEFAULT))
                .overlay(left.getAlias(), left.inferenceContext())
                .overlay(right.getAlias(), right.inferenceContext());
    }

    @Override
    public void collectMaterializationDependencies(final Map<Name, LinkableSchema> out) {

        join.collectMaterializationDependencies(out);
    }

    @Override
    public void collectDependencies(final Map<Name, Schema<?>> out) {

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
}
