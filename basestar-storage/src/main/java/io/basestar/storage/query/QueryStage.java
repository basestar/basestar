package io.basestar.storage.query;

import io.basestar.expression.Expression;
import io.basestar.schema.Layout;
import io.basestar.util.Name;
import io.basestar.util.Sort;

import java.util.List;
import java.util.Set;

public interface QueryStage {

    Layout getLayout();

    default Set<Name> getExpand() {

        return getLayout().getExpand();
    }

    List<Sort> getSort();

    Expression getFilter();
}
