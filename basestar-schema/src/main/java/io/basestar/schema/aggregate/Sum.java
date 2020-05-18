package io.basestar.schema.aggregate;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import io.basestar.expression.Expression;
import io.basestar.jackson.serde.ExpressionDeseriaizer;
import lombok.Builder;
import lombok.Data;

@Data
@JsonDeserialize(builder = Sum.Builder.class)
@Builder(builderClassName = "Builder", setterPrefix = "set")
public class Sum implements Aggregate {

    public static final String TYPE = "sum";

    private final Expression input;

    private final Expression output;

    @Override
    public <T> T visit(final AggregateVisitor<T> visitor) {

        return visitor.visitSum(this);
    }

    @JsonPOJOBuilder(withPrefix = "set")
    public static class Builder {

        @JsonDeserialize(using = ExpressionDeseriaizer.class)
        private Expression input;

        @JsonDeserialize(using = ExpressionDeseriaizer.class)
        private Expression output;
    }
}
