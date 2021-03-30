package io.basestar.schema.from;

import io.basestar.expression.Expression;
import io.basestar.expression.constant.NameConstant;
import io.basestar.schema.*;
import io.basestar.schema.exception.SchemaValidationException;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseBinary;
import io.basestar.util.BinaryKey;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Sort;
import lombok.Getter;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Getter
public class FromUnion implements From {

    @Nonnull
    private final List<From> union;

    private final String as;

    public FromUnion(final Schema.Resolver.Constructing resolver, final From.Descriptor from) {

        this.union = Immutable.transform(from.getUnion(), v -> v.build(resolver));
        this.as = from.getAs();
    }

    @Override
    public From.Descriptor descriptor() {

        return new From.Descriptor() {

            @Override
            public List<From.Descriptor> getUnion() {

                return Immutable.transform(union, From::descriptor);
            }

            @Override
            public String getAs() {

                return as;
            }
        };
    }

    @Override
    public InferenceContext inferenceContext() {

        return new InferenceContext.Union(union.stream().map(From::inferenceContext).collect(Collectors.toList()));
    }

    @Override
    public void collectMaterializationDependencies(final Map<Name, LinkableSchema> out) {

        union.forEach(v -> v.collectMaterializationDependencies(out));
    }

    @Override
    public void collectDependencies(final Map<Name, Schema<?>> out) {

        union.forEach(v -> v.collectDependencies(out));
    }

    @Override
    public Use<?> typeOfId() {

        // FIXME: must validate that all clauses have the same id type
        final From first = union.get(0);
        return first.typeOfId();
    }

    @Override
    public void validateProperty(final Property property) {

        if (property.getExpression() == null) {
            throw new SchemaValidationException(property.getQualifiedName(), "Every view property must have an expression");
        }
    }

    @Override
    public BinaryKey id(final Map<String, Object> row) {

        // FIXME: must validate that all clauses have the same id type
        final From first = union.get(0);
        return first.id(row);
    }

    @Override
    public boolean isCompatibleBucketing(final List<Bucketing> other) {

        return union.stream().allMatch(v -> v.isCompatibleBucketing(other));
    }

    @Override
    public List<FromSchema> schemas() {

        return union.stream().flatMap(from -> from.schemas().stream())
                .collect(Collectors.toList());
    }

    @Override
    public Expression id() {

        // FIXME: must validate that all clauses have the same id expression
        final From first = union.get(0);
        return first.id();
    }
}
