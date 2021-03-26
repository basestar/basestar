package io.basestar.schema.from;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.basestar.expression.Expression;
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.Schema;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import lombok.Data;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import java.util.Map;

@Data
public class Join {

    public enum Type {

        INNER,
        LEFT,
        RIGHT
    }

    @Nonnull
    private final From left;

    @Nonnull
    private final From right;

    private final Expression on;

    @Nonnull
    private final Type type;

    public Join(final Schema.Resolver.Constructing resolver, final Descriptor builder) {

        this.left = Nullsafe.require(builder.getLeft().build(resolver));
        this.right = Nullsafe.require(builder.getLeft().build(resolver));
        this.on = Nullsafe.require(builder.getOn());
        this.type = Nullsafe.orDefault(builder.getType(), Type.INNER);
    }

    public void collectMaterializationDependencies(final Map<Name, LinkableSchema> out) {

        left.collectMaterializationDependencies(out);
        right.collectMaterializationDependencies(out);
    }

    public void collectDependencies(final Map<Name, Schema<?>> out) {

        left.collectDependencies(out);
        right.collectDependencies(out);
    }

    public Descriptor descriptor() {

        return new Descriptor() {
            @Override
            public From.Descriptor getLeft() {

                return left.descriptor();
            }

            @Override
            public From.Descriptor getRight() {

                return right.descriptor();
            }

            @Override
            public Expression getOn() {

                return on;
            }

            @Override
            public Type getType() {

                return type;
            }
        };
    }

    @JsonDeserialize(as = From.Builder.class)
    interface Descriptor {

        From.Descriptor getLeft();

        From.Descriptor getRight();

        Expression getOn();

        Type getType();

        default Join build(final Schema.Resolver.Constructing resolver) {

            return new Join(resolver, this);
        }
    }

    @Data
    @Accessors(chain = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Builder implements Descriptor {

        private From.Descriptor left;

        private From.Descriptor right;

        private Expression on;

        private Type type;
    }
}