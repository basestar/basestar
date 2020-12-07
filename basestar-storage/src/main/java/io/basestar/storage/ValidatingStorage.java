package io.basestar.storage;

import io.basestar.schema.Consistency;
import io.basestar.schema.History;
import io.basestar.schema.Index;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.exception.UnsupportedConsistencyException;

import java.util.Map;

public interface ValidatingStorage extends Storage {

    @Override
    default void validate(final ObjectSchema schema) {

        final StorageTraits storageTraits = storageTraits(schema);
        final Consistency bestHistoryWrite = storageTraits.getHistoryConsistency();
        final Consistency bestSingleIndexWrite = storageTraits.getSingleValueIndexConsistency();
        final Consistency bestMultiIndexWrite = storageTraits.getMultiValueIndexConsistency();

        final History history = schema.getHistory();
        if (history.isEnabled()) {
            final Consistency requested = history.getConsistency();
            if (requested != null && requested.isStronger(bestHistoryWrite)) {
                throw new UnsupportedConsistencyException(schema.getQualifiedName() + ".history", name(), bestHistoryWrite, requested);
            }
        }
        for (final Map.Entry<String, Index> entry : schema.getIndexes().entrySet()) {
            final Index index = entry.getValue();
            final Consistency requested = index.getConsistency();
            if (requested != null) {
                final Consistency best = index.isMultiValue() ? bestMultiIndexWrite : bestSingleIndexWrite;
                if (requested.isStronger(best)) {
                    throw new UnsupportedConsistencyException(schema.getQualifiedName() + "." + entry.getKey(), name(), best, requested);
                }
            }
        }
    }
}
