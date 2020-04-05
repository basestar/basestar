package io.basestar.schema;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import io.basestar.expression.Expression;
import io.basestar.jackson.serde.ExpressionDeseriaizer;
import io.basestar.util.Nullsafe;
import io.basestar.util.Path;
import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;

@Getter
public class Constraint implements Named, Described, Serializable {

    public static final String REQUIRED = "required";

    public static final String IMMUTABLE = "immutable";

    @Nonnull
    private final String name;

    @Nullable
    private final String description;

    @Nonnull
    private final Expression expression;

    @Data
    @Accessors(chain = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Builder implements Described {

        @Nullable
        private String description;

        @Nullable
        @JsonSerialize(using = ToStringSerializer.class)
        @JsonDeserialize(using = ExpressionDeseriaizer.class)
        private Expression expression;

        public Constraint build(final String name) {

            return new Constraint(this, name);
        }
    }

    public static Builder builder() {

        return new Builder();
    }

    private Constraint(final Builder builder, final String name) {

        this.name = name;
        this.description = builder.getDescription();
        this.expression = Nullsafe.of(builder.getExpression());
    }

    @Data
    public static class Violation {

        @JsonSerialize(using = ToStringSerializer.class)
        private final Path path;

        private final String constraint;
    }
}
