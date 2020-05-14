package io.basestar.schema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class Ref {

    private final String schema;

    private final String id;

    @JsonCreator
    public static Ref of(@JsonProperty("schema") final String schema, @JsonProperty("id") final String id) {

        return new Ref(schema, id);
    }
}
