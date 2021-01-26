package io.basestar.database.options;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import io.basestar.auth.Caller;
import io.basestar.database.Database;
import io.basestar.util.Name;
import lombok.Builder;
import lombok.Data;

import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

@Data
@Builder(toBuilder = true, builderClassName = "Builder", setterPrefix = "set")
@JsonDeserialize(builder = RepairOptions.Builder.class)
public class RepairOptions implements Options {

    public static final String TYPE = "repair";

    private final Name schema;

    @Nullable
    private final String index;


    @Override
    public CompletableFuture<?> apply(final Caller caller, final Database database) {

        throw new UnsupportedOperationException();
    }

    @JsonPOJOBuilder(withPrefix = "set")
    public static class Builder {

    }
}
