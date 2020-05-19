package io.basestar.database.options;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import io.basestar.database.serde.AggregateGroupDeserializer;
import io.basestar.expression.Expression;
import io.basestar.jackson.serde.ExpressionDeseriaizer;
import io.basestar.schema.aggregate.Aggregate;
import lombok.Builder;
import lombok.Data;

import java.util.Map;

@Data
@JsonDeserialize(builder = AggregateOptions.Builder.class)
@Builder(toBuilder = true, builderClassName = "Builder", setterPrefix = "set")
public class AggregateOptions {

    public static final String TYPE = "aggregate";

    private final String schema;

    private final Expression filter;

    private final Map<String, Expression> group;

    private final Map<String, Aggregate> aggregate;

    @JsonPOJOBuilder(withPrefix = "set")
    public static class Builder {

        private String schema;

        @JsonDeserialize(using = ExpressionDeseriaizer.class)
        private Expression filter;

        @JsonDeserialize(using = AggregateGroupDeserializer.class)
        private Map<String, Expression> group;

        private Map<String, Aggregate> aggregate;
    }
}
