package io.basestar.schema.layout;

import io.basestar.schema.use.Use;

import java.util.Map;

public interface Layout {

    Map<String, Use<?>> layout();

    Map<String, Object> applyLayout(Map<String, Object> object);

    Map<String, Object> unapplyLayout(Map<String, Object> object);
}
