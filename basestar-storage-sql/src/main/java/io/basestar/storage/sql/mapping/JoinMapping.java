package io.basestar.storage.sql.mapping;

import java.util.concurrent.locks.Condition;

public interface JoinMapping {

    SchemaMapping schemaMapping();

    Condition condition();
}
