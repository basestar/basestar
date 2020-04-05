package io.basestar.database.options;

import io.basestar.util.Path;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.Set;

@Data
@Accessors(chain = true)
public class ReadOptions {

    private Set<Path> expand;

    private Set<Path> projection;

    private Long version;
}
