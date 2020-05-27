package io.basestar.stream;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.basestar.auth.Caller;
import io.basestar.expression.Expression;
import io.basestar.util.Path;
import lombok.Data;

import java.util.List;
import java.util.Set;

@Data
public class Subscription {

    private String id;

    private String sub;

    private String channel;

    @JsonDeserialize(builder = Caller.Builder.class)
    private Caller caller;

    private Expression expression;

    private Set<Path> expand;

    @Data
    public static class Key {

        private final String schema;

        private final String index;

        private final List<Object> partition;
    }
}
