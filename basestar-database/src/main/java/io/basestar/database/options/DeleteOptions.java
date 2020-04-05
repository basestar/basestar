package io.basestar.database.options;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class DeleteOptions {

    private Long version;
}
