package io.basestar.database.options;

import io.basestar.util.Name;
import lombok.Builder;
import lombok.Data;

import javax.annotation.Nullable;

@Data
@Builder(toBuilder = true, builderClassName = "Builder")
public class RepairOptions implements Options {

    public static final String TYPE = "repair";

    private final Name schema;

    @Nullable
    private final String index;
}
