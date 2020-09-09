package io.basestar.codegen;

import io.basestar.util.Name;
import io.basestar.util.Path;
import lombok.Builder;
import lombok.Data;

import java.util.Map;

@Data
@Builder
public class CodegenSettings {

    private final String packageName;

    private final Map<Name, Codebehind> codebehind;

    private final Path codebehindPath;
}
