package io.basestar.schema.from;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.Schema;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import lombok.Data;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Map;

@Data
public class Join implements Serializable {

    public enum Type {

        INNER,
        LEFT_OUTER,
        RIGHT_OUTER,
        FULL_OUTER
    }

    @Nonnull
    private final From left;

    @Nonnull
    private final From right;

    @Nonnull
    private final Type type;

    private final Expression on;

    public Join(final From left, final From right, final Type type, final Expression on) {

        this.left = Nullsafe.require(left);
        this.right = Nullsafe.require(right);
        this.type = Nullsafe.require(type);
        this.on = Nullsafe.require(on);
    }

    public Join(final Schema.Resolver.Constructing resolver, final Context context, final Descriptor builder) {

        this.left = Nullsafe.require(builder.getLeft().build(resolver, context));
        this.right = Nullsafe.require(builder.getRight().build(resolver, context));
        this.type = Nullsafe.orDefault(builder.getType(), Type.INNER);
        this.on = Nullsafe.require(builder.getOn()).bind(context);
    }

    public void collectMaterializationDependencies(final Map<Name, Schema<?>> out) {

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
            public Type getType() {

                return type;
            }

            @Override
            public Expression getOn() {

                return on;
            }
        };
    }

    @JsonDeserialize(as = Builder.class)
    interface Descriptor {

        From.Descriptor getLeft();

        From.Descriptor getRight();

        Type getType();

        Expression getOn();

        default Join build(final Schema.Resolver.Constructing resolver, final Context context) {

            return new Join(resolver, context, this);
        }
    }

    @Data
    @Accessors(chain = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Builder implements Descriptor {

        private From.Descriptor left;

        private From.Descriptor right;

        private Type type;

        private Expression on;
    }
}