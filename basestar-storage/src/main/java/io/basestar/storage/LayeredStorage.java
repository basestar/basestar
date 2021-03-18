package io.basestar.storage;

import io.basestar.expression.Expression;
import io.basestar.schema.*;
import io.basestar.util.Name;
import io.basestar.util.Pager;
import io.basestar.util.Sort;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface LayeredStorage extends Storage {

    @Override
    ReadTransaction read(Consistency consistency);

    @Override
    WriteTransaction write(Consistency consistency, Versioning versioning);

    Pager<Map<String, Object>> queryObject(Consistency consistency, ObjectSchema schema, Expression query, List<Sort> sort, Set<Name> expand);

    Pager<Map<String, Object>> queryInterface(Consistency consistency, InterfaceSchema schema, Expression query, List<Sort> sort, Set<Name> expand);

    Pager<Map<String, Object>> queryView(Consistency consistency, ViewSchema schema, Expression query, List<Sort> sort, Set<Name> expand);

    @Override
    default Pager<Map<String, Object>> query(final Consistency consistency, final LinkableSchema schema, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        if(schema instanceof ViewSchema) {
            return queryView(consistency, (ViewSchema)schema, query, sort, expand);
        } else if(schema instanceof InterfaceSchema) {
            return queryInterface(consistency, (InterfaceSchema)schema, query, sort, expand);
        } else {
            return queryObject(consistency, (ObjectSchema)schema, query, sort, expand);
        }
    }

    interface ReadTransaction extends Storage.ReadTransaction {

        @Override
        default Storage.ReadTransaction get(final ReferableSchema schema, final String id, final Set<Name> expand) {

            if(schema instanceof ObjectSchema) {
                return getObject((ObjectSchema)schema, id, expand);
            } else {
                return getInterface((InterfaceSchema) schema, id, expand);
            }
        }

        @Override
        default Storage.ReadTransaction getVersion(final ReferableSchema schema, final String id, final long version, final Set<Name> expand) {

            if(schema instanceof ObjectSchema) {
                return getObjectVersion((ObjectSchema)schema, id, version, expand);
            } else {
                return getInterfaceVersion((InterfaceSchema) schema, id, version, expand);
            }
        }

        Storage.ReadTransaction getObject(ObjectSchema schema, String id, Set<Name> expand);

        Storage.ReadTransaction getObjectVersion(ObjectSchema schema, String id, long version, Set<Name> expand);

        Storage.ReadTransaction getInterface(InterfaceSchema schema, String id, Set<Name> expand);

        Storage.ReadTransaction getInterfaceVersion(InterfaceSchema schema, String id, long version, Set<Name> expand);
    }

    interface WriteTransaction extends Storage.WriteTransaction {

        void createObjectLayer(ReferableSchema schema, String id, Map<String, Object> after);

        void updateObjectLayer(ReferableSchema schema, String id, Map<String, Object> before, Map<String, Object> after);

        void deleteObjectLayer(ReferableSchema schema, String id, Map<String, Object> before);

        void writeHistoryLayer(ReferableSchema schema, String id, Map<String, Object> after);
    }
}
