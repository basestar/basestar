package io.basestar.storage.sql.mapping;

import io.basestar.storage.sql.resolver.ExpressionResolver;
import io.basestar.storage.sql.resolver.RecordResolver;
import io.basestar.storage.sql.resolver.TableResolver;
import io.basestar.util.Name;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Table;

import java.util.Optional;
import java.util.Set;

public interface LinkMapping {

    Set<Name> supportedExpand(Set<Name> expand);

    Object fromRecord(Name qualifiedName, RecordResolver record, Set<Name> expand);

    Optional<Field<?>> nestedField(Name qualifiedName, Name name);

    Table<?> join(Name qualifiedName, DSLContext context, TableResolver tableResolver, ExpressionResolver expressionResolver, Table<?> table, Set<Name> expand);
}
