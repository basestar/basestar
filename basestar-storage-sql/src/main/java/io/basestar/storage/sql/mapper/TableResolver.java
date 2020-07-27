package io.basestar.storage.sql.mapper;

import io.basestar.schema.ObjectSchema;
import org.jooq.Record;
import org.jooq.Table;

public interface TableResolver {

    Table<Record> table(ObjectSchema schema);
}
