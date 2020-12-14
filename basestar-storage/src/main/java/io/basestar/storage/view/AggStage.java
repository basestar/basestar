package io.basestar.storage.view;

import io.basestar.expression.aggregate.Aggregate;
import io.basestar.util.Text;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class AggStage implements QueryStage {

    private final QueryStage input;

    private final List<String> group;

    private final Map<String, Aggregate> aggregates;

    @Override
    public String toString() {

        return "- Aggregate:\n" + Text.indent(input.toString());
    }

    @Override
    public boolean isSorted() {

        return true;
    }
}
