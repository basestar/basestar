package io.basestar.storage.view;

import io.basestar.expression.Expression;
import io.basestar.util.Text;
import lombok.Data;

@Data
public class FilterStage implements QueryStage {

    private final QueryStage input;

    private final Expression filter;

    @Override
    public String toString() {

        return "- Filter:\n" + Text.indent(input.toString());
    }

    @Override
    public boolean isSorted() {

        return input.isSorted();
    }
}
