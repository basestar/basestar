package io.basestar.schema.use;

import io.basestar.schema.Instance;
import io.basestar.schema.layout.Layout;

public interface UseLayout extends Use<Instance> {

    Layout getSchema();
}
