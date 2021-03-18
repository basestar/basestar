package io.basestar.schema.from;

import io.basestar.schema.Layout;
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.Property;
import io.basestar.schema.Schema;
import io.basestar.schema.exception.SchemaValidationException;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.use.Use;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Sort;
import lombok.Getter;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Getter
public
class FromUnion implements From {

    @Nonnull
    private final List<From> union;

    public FromUnion(final Schema.Resolver.Constructing resolver, final From.Descriptor from) {

        this.union = Immutable.transform(from.getUnion(), v -> v.build(resolver));
    }

    @Override
    public From.Descriptor descriptor() {

        return new From.Descriptor() {
            @Override
            public Name getSchema() {

                return null;
            }

            @Override
            public List<Sort> getSort() {

                return null;
            }

            @Override
            public Set<Name> getExpand() {

                return null;
            }

            @Override
            public String getSql() {

                return null;
            }

            @Override
            public Map<String, From.Descriptor> getUsing() {

                return null;
            }

            @Override
            public List<String> getPrimaryKey() {

                return null;
            }

            @Override
            public List<From.Descriptor> getUnion() {

                return Immutable.transform(union, From::descriptor);
            }
        };
    }

    @Override
    public InferenceContext inferenceContext() {

        throw new UnsupportedOperationException();
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

        throw new UnsupportedOperationException();
    }

    @Override
    public void validateProperty(final Property property) {

        if (property.getExpression() == null) {
            throw new SchemaValidationException(property.getQualifiedName(), "Every view property must have an expression");
        }
    }
}
