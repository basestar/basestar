package io.basestar.storage.sql.strategy;

import io.basestar.schema.LinkableSchema;
import io.basestar.schema.ViewSchema;
import io.basestar.storage.sql.SQLDialect;

public interface EntityTypeStrategy {

    Default DEFAULT = new Default();

    EntityType entityType(io.basestar.storage.sql.SQLDialect dialect, LinkableSchema schema);

    public class Default implements EntityTypeStrategy {

        @Override
        public EntityType entityType(final SQLDialect dialect, final LinkableSchema schema) {

            if (schema instanceof ViewSchema) {
                final ViewSchema view = (ViewSchema) schema;
                if (view.isMaterialized()) {
                    return dialect.supportsMaterializedView(view) ? EntityType.MATERIALIZED_VIEW : EntityType.TABLE;
                } else {
                    return EntityType.VIEW;
                }
            } else {
                return EntityType.TABLE;
            }
        }
    }
}