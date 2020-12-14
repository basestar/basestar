package io.basestar.storage.view;

import io.basestar.expression.Expression;
import io.basestar.util.Text;
import lombok.Data;

import java.util.Map;

@Data
public class MapStage implements QueryStage {

    private final QueryStage input;

    private final Map<String, Expression> outputs;

    @Override
    public String toString() {

        return "- Map:\n" + Text.indent(input.toString());
    }

    @Override
    public boolean isSorted() {

        return input.isSorted();
    }
}
