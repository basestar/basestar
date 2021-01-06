package io.basestar.schema.util;

import io.basestar.schema.LinkableSchema;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface BucketStats {

    enum Operation {

        CREATE,
        UPDATE,
        DELETE
    }

    Optional<Map<Bucket, Set<Operation>>> stats(LinkableSchema schema);
}
