package io.basestar.storage.sql.resolver;

public interface ResolvedTable extends ColumnResolver {

    org.jooq.Table<?> getTable();
}
