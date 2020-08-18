package io.basestar.schema.layout;

import io.basestar.schema.use.Use;
import io.basestar.util.Name;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

public interface Layout extends Serializable {

    Map<String, Use<?>> layout(Set<Name> expand);

    Map<String, Object> applyLayout(Set<Name> expand, Map<String, Object> object);

    Map<String, Object> unapplyLayout(Set<Name> expand, Map<String, Object> object);
}
