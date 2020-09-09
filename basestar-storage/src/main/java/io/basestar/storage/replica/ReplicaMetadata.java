package io.basestar.storage.replica;

import io.basestar.schema.Instance;
import io.basestar.schema.Reserved;
import io.basestar.storage.Metadata;
import lombok.Data;

import java.util.Map;

@Data
public class ReplicaMetadata implements Metadata {

    private final Metadata primary;

    private final Metadata replica;

    public static ReplicaMetadata wrap(final Map<String, Object> primary, final Map<String, Object> replica) {

        final Metadata pri = primary == null ? null : Instance.get(primary, Reserved.META, Metadata.class);
        final Metadata rep = replica == null ? null : Instance.get(replica, Reserved.META, Metadata.class);
        return new ReplicaMetadata(pri, rep);
    }

    public static Map<String, Object> unwrapPrimary(final Map<String, Object> object) {

        final ReplicaMetadata metadata = object == null ? null : Instance.get(object, Reserved.META, ReplicaMetadata.class);
        if(metadata != null) {
            return Instance.with(object, Reserved.META, metadata.getPrimary());
        } else {
            return object;
        }
    }

    public static Map<String, Object> unwrapReplica(final Map<String, Object> object) {

        final ReplicaMetadata metadata = object == null ? null : Instance.get(object, Reserved.META, ReplicaMetadata.class);
        if(metadata != null) {
            return Instance.with(object, Reserved.META, metadata.getPrimary());
        } else {
            return object;
        }
    }
}
