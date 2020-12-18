package io.basestar.storage.view;

import io.basestar.expression.aggregate.Aggregate;
import io.basestar.schema.Layout;
import io.basestar.util.Text;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class AggStage implements QueryStage {

    private final QueryStage input;

    private final List<String> group;

    private final Map<String, Aggregate> aggregates;

    private final Layout outputLayout;

    @Override
    public String toString() {

        return "- Aggregate(group=" + group + ", aggregates=" + aggregates + "):\n" + Text.indent(input.toString());
    }

    @Override
    public Layout outputLayout() {

        return outputLayout;
    }

    @Override
    public <T> T visit(final Visitor<T> visitor) {

        return visitor.visitAgg(this);
    }
}
