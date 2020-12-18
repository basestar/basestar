package io.basestar.storage.view;

import io.basestar.schema.Layout;
import io.basestar.util.Sort;
import io.basestar.util.Text;
import lombok.Data;

import java.util.List;

@Data
public class SortStage implements QueryStage {

    private final QueryStage input;

    private final List<Sort> sort;

    @Override
    public String toString() {

        return "- Sort(using=" + sort + "):\n" + Text.indent(input.toString());
    }

    @Override
    public Layout outputLayout() {

        return input.outputLayout();
    }

    @Override
    public <T> T visit(final Visitor<T> visitor) {

        return visitor.visitSort(this);
    }
}
