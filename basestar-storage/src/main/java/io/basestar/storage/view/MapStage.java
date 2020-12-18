package io.basestar.storage.view;

import io.basestar.expression.Expression;
import io.basestar.schema.Layout;
import io.basestar.util.Text;
import lombok.Data;

import java.util.Map;

@Data
public class MapStage implements QueryStage {

    private final QueryStage input;

    private final Map<String, Expression> outputs;

    private final Layout outputLayout;

    @Override
    public String toString() {

        return "- Map(expressions=" + outputs + "):\n" + Text.indent(input.toString());
    }

    @Override
    public <T> T visit(final Visitor<T> visitor) {

        return visitor.visitMap(this);
    }

    @Override
    public Layout outputLayout() {

        return outputLayout;
//        final InferenceContext context = InferenceContext.empty().with(input.outputLayout().getSchema());
//        final InferenceVisitor visitor = new InferenceVisitor(context);
//        final Map<String, Use<?>> schema = outputs.entrySet().stream().collect(Collectors.toMap(
//                Map.Entry::getKey,
//                e -> visitor.visit(e.getValue())
//        ));
//        return Layout.simple(schema, ImmutableSet.of());
    }
}
