package io.basestar.codegen;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class CodegenSettings {

    private final String packageName;
}
