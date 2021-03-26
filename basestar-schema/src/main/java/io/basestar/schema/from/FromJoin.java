package io.basestar.schema.from;

import io.basestar.schema.LinkableSchema;
import io.basestar.schema.Property;
import io.basestar.schema.Schema;
import io.basestar.schema.exception.SchemaValidationException;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.use.Use;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import lombok.Data;
import lombok.Getter;

import java.util.Map;

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

        throw new UnsupportedOperationException();
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

        throw new UnsupportedOperationException();
    }

    @Override
    public void validateProperty(final Property property) {

        if (property.getExpression() == null) {
            throw new SchemaValidationException(property.getQualifiedName(), "Every view property must have an expression");
        }
    }
}
