package io.basestar.schema.from;

import com.google.common.collect.ImmutableMap;
import io.basestar.expression.Expression;
import io.basestar.expression.arithmetic.Add;
import io.basestar.schema.*;
import io.basestar.schema.exception.SchemaValidationException;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseBinary;
import io.basestar.schema.use.UseString;
import io.basestar.util.BinaryKey;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import lombok.Getter;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Getter
public class FromJoin implements From {

    private final Join join;

    private final String as;

    public FromJoin(final Schema.Resolver.Constructing resolver, final From.Descriptor from) {

        this.join = Nullsafe.require(from.getJoin()).build(resolver);
        this.as = from.getAs();
    }

    @Override
    public From.Descriptor descriptor() {

        return new From.Descriptor() {
            @Override
            public Join.Descriptor getJoin() {

                return join.descriptor();
            }

            @Override
            public String getAs() {

                return as;
            }
        };
    }

    @Override
    public InferenceContext inferenceContext() {

        // Temp exceptions, should support anon left/right sides as long as there are no naming conflicts
        if(join.getLeft().getAs() == null) {
           throw new IllegalStateException("Left side of join must be named (with as=)");
        }
        if(join.getRight().getAs() == null) {
            throw new IllegalStateException("Right side of join must be named (with as=)");
        }

        return InferenceContext.from(ImmutableMap.of(ViewSchema.ID, UseString.DEFAULT))
                .overlay(join.getLeft().getAs(), join.getLeft().inferenceContext())
                .overlay(join.getRight().getAs(), join.getRight().inferenceContext());
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
    public void validateProperty(final Property property) {

        if (property.getExpression() == null) {
            throw new SchemaValidationException(property.getQualifiedName(), "Every view property must have an expression");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public BinaryKey id(final Map<String, Object> row) {

        final BinaryKey left = join.getLeft().id((Map<String, Object>)row.get(join.getLeft().getAs()));
        final BinaryKey right = join.getRight().id((Map<String, Object>)row.get(join.getRight().getAs()));
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
}
