package io.basestar.storage.sql.mapping;

import io.basestar.schema.LinkableSchema;
import io.basestar.util.Name;

import java.util.Map;

public class JoinTree {

    private LinkableSchema schema;

    private Map<Name, JoinTree> refs;

    private Map<Name, JoinTree> links;
}
