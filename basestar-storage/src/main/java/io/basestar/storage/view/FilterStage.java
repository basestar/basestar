package io.basestar.storage.view;

import io.basestar.expression.Expression;
import io.basestar.schema.Layout;
import io.basestar.util.Text;
import lombok.Data;

@Data
public class FilterStage implements QueryStage {

    private final QueryStage input;

    private final Expression condition;

    @Override
    public String toString() {

        return "- Filter(condition=" + condition + "):\n" + Text.indent(input.toString());
    }

    @Override
    public Layout outputLayout() {

        return input.outputLayout();
    }

    @Override
    public <T> T visit(final Visitor<T> visitor) {

        return visitor.visitFilter(this);
    }
}
