package io.basestar.storage.sql.mapper;

import io.basestar.schema.ReferableSchema;
import org.jooq.Record;
import org.jooq.Table;

public interface TableResolver {

    Table<Record> table(ReferableSchema schema);
}
