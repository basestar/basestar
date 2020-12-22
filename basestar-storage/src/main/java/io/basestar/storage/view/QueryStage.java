package io.basestar.storage.view;

import io.basestar.expression.Expression;
import io.basestar.schema.Layout;
import io.basestar.util.Sort;

import java.util.List;

public interface QueryStage {

    Layout getOutputLayout();

    List<Sort> getSort();

    Expression getFilter();
}
