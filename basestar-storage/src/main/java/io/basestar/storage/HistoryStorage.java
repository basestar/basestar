package io.basestar.storage;

import io.basestar.schema.Consistency;
import io.basestar.schema.ObjectSchema;

import java.util.Map;

public interface HistoryStorage extends Storage {

    @Override
    WriteTransaction write(Consistency consistency, Versioning versioning);

    interface WriteTransaction extends Storage.WriteTransaction {

        WriteTransaction writeHistory(ObjectSchema schema, String id, Map<String, Object> after);
    }
}