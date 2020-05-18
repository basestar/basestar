package io.basestar.schema.aggregate;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import io.basestar.expression.Expression;
import io.basestar.jackson.serde.ExpressionDeseriaizer;
import lombok.Builder;
import lombok.Data;

@Data
@JsonDeserialize(builder = Count.Builder.class)
@Builder(builderClassName = "Builder", setterPrefix = "set")
public class Count implements Aggregate {

    public static final String TYPE = "count";

    private final Expression output;

    @Override
    public <T> T visit(final AggregateVisitor<T> visitor) {

        return visitor.visitCount(this);
    }

    @JsonPOJOBuilder(withPrefix = "set")
    public static class Builder {

        @JsonDeserialize(using = ExpressionDeseriaizer.class)
        private Expression output;
    }
}
