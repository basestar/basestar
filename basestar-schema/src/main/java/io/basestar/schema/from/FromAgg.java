package io.basestar.schema.from;

import io.basestar.expression.Expression;
import io.basestar.schema.Bucketing;
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.Schema;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.expression.TypedExpression;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseBinary;
import io.basestar.util.BinaryKey;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class FromAgg implements From {

    private final From from;

    private final List<String> group;

    private final Map<String, Expression> agg;

    public FromAgg(final From from, final List<String> group, final Map<String, Expression> agg) {

        this.from = Nullsafe.require(from);
        this.group = Immutable.list(group);
        this.agg = Immutable.map(agg);
    }

    @Override
    public Descriptor descriptor() {

        return new Descriptor.Defaulting() {
            @Override
            public Descriptor getFrom() {

                return from.descriptor();
            }

            @Override
            public Map<String, Expression> getSelect() {

                return agg;
            }

            @Override
            public List<String> getGroup() {

                return group;
            }
        };
    }

    @Override
    public InferenceContext inferenceContext() {

        final InferenceContext context = from.inferenceContext();
        return InferenceContext.from(Immutable.transformValues(agg, (k, v) -> context.typeOf(v)));
    }

    @Override
    public void collectMaterializationDependencies(final Map<Name, LinkableSchema> out) {

        from.collectMaterializationDependencies(out);
    }

    @Override
    public void collectDependencies(final Map<Name, Schema<?>> out) {

        from.collectDependencies(out);
    }

    public Map<String, TypedExpression<?>> typedAgg() {

        final InferenceContext context = from.inferenceContext();
        return Immutable.transformValues(agg, (k, v) -> TypedExpression.from(v, context.typeOf(v)));
    }

    @Override
    public Expression id() {

        return null;
    }

    @Override
    public Use<?> typeOfId() {

        return UseBinary.DEFAULT;
    }

    @Override
    public Map<String, Use<?>> getProperties() {

        final InferenceContext context = from.inferenceContext();
        final Map<String, Use<?>> properties = new HashMap<>();
        agg.keySet().forEach(k -> properties.put(k, context.requireTypeOf(Name.of(k))));
        return properties;
    }

    @Override
    public BinaryKey id(final Map<String, Object> row) {

        return null;
    }

    @Override
    public boolean isCompatibleBucketing(final List<Bucketing> other) {

        return false;
    }

    @Override
    public List<FromSchema> schemas() {

        return from.schemas();
    }

    @Override
    public boolean isExternal() {

        return from.isExternal();
    }

    @Override
    public <T> T visit(final FromVisitor<T> visitor) {

        return visitor.visitAgg(this);
    }
}
