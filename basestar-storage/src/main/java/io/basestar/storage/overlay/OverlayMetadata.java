package io.basestar.storage.overlay;

import io.basestar.schema.Instance;
import io.basestar.schema.Reserved;
import io.basestar.storage.Metadata;
import lombok.Data;

import java.util.Map;

@Data
public class OverlayMetadata implements Metadata {

    private final Metadata baseline;

    private final Metadata overlay;

    public static OverlayMetadata wrap(final Map<String, Object> baseline, final Map<String, Object> overlay) {

        final Metadata base = baseline == null ? null : Instance.get(baseline, Reserved.META, Metadata.class);
        final Metadata over = overlay == null ? null : Instance.get(overlay, Reserved.META, Metadata.class);
        return new OverlayMetadata(base, over);
    }

    public static Map<String, Object> unwrapOverlay(final Map<String, Object> object) {

        final OverlayMetadata metadata = object == null ? null : Instance.get(object, Reserved.META, OverlayMetadata.class);
        if(metadata != null) {
            return Instance.with(object, Reserved.META, metadata.getOverlay());
        } else {
            return object;
        }
    }
}
