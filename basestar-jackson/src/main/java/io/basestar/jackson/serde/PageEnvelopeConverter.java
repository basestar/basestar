package io.basestar.jackson.serde;

import com.fasterxml.jackson.databind.util.StdConverter;
import io.basestar.util.Page;

/**
 * Page items should be output wrapped in an envelope, with stats and paging.
 *
 * @param <T>
 */
public class PageEnvelopeConverter<T> extends StdConverter<Page<T>, Page.Envelope<T>> {

    @Override
    public Page.Envelope<T> convert(final Page<T> value) {
        return value.toEnvelope();
    }
}
