package io.basestar.schema.from;

import io.basestar.expression.Expression;
import io.basestar.schema.Bucketing;
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.Schema;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.expression.TypedExpression;
import io.basestar.schema.use.Use;
import io.basestar.util.BinaryKey;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class FromMap implements From {

    private final From from;

    private final Map<String, Expression> map;

    public FromMap(final From from, final Map<String, Expression> map) {

        this.from = Nullsafe.require(from);
        this.map = Immutable.map(map);
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

                return map;
            }
        };
    }

    @Override
    public InferenceContext inferenceContext() {

        final InferenceContext context = from.inferenceContext();
        return InferenceContext.from(Immutable.transformValues(map, (k, v) -> context.typeOf(v)));
    }

    public Map<String, TypedExpression<?>> typedMap() {

        final InferenceContext context = from.inferenceContext();
        return Immutable.transformValues(map, (k, v) -> TypedExpression.from(v, context.typeOf(v)));
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

        final InferenceContext context = inferenceContext();
        return Immutable.transformValues(map, (k, v) -> context.typeOf(v));
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

        return visitor.visitMap(this);
    }
}
