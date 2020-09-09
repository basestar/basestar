package io.basestar.codegen;

import io.basestar.util.Name;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Codebehind {

    private final Name name;

    private final boolean generate;
}
