package io.basestar.storage;

import io.basestar.expression.Expression;
import io.basestar.schema.Consistency;
import io.basestar.schema.Index;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.ReferableSchema;
import io.basestar.util.Name;
import io.basestar.util.Pager;
import io.basestar.util.Sort;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public interface IndexStorage extends Storage {

    @Override
    WriteTransaction write(Consistency consistency, Versioning versioning);

    Pager<Map<String, Object>> queryIndex(ObjectSchema schema, Index index, Expression expression, List<Sort> sort, Set<Name> expand);

    interface WriteTransaction extends Storage.WriteTransaction {

        WriteTransaction createIndexes(ReferableSchema schema, List<Index> indexes, String id, Map<String, Object> after);

        WriteTransaction updateIndexes(ReferableSchema schema, List<Index> indexes, String id, Map<String, Object> before, Map<String, Object> after);

        WriteTransaction deleteIndexes(ReferableSchema schema, List<Index> indexes, String id, Map<String, Object> before);
    }

    static List<Index> getAsyncIndexes(final StorageTraits traits, final ObjectSchema schema) {

        return schema.getIndexes().values().stream()
                .filter(index -> index.getConsistency(traits.getIndexConsistency(index.isMultiValue())).isAsync())
                .collect(Collectors.toList());
    }

    static List<Index> getSyncIndexes(final StorageTraits traits, final ObjectSchema schema) {

        return schema.getIndexes().values().stream()
                .filter(index -> !index.getConsistency(traits.getIndexConsistency(index.isMultiValue())).isAsync())
                .collect(Collectors.toList());
    }
}