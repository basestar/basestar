package io.basestar.storage.view;

import io.basestar.expression.Expression;
import io.basestar.util.Text;
import lombok.Data;

import java.util.Map;
import java.util.stream.Collectors;

@Data
public class ExpandStage implements QueryStage {

    private final QueryStage input;

    private final Map<String, Join> joins;

    @Data
    public static class Join {

        private final QueryStage input;

        private final Expression condition;

        @Override
        public String toString() {

            return "- Join:\n" + Text.indent(input.toString());
        }
    }

    @Override
    public String toString() {

        return "- Expand:\n" + Text.indent(input.toString() + "\n" + joins.values().stream()
                .map(join -> Text.indent(join.toString()))
                .collect(Collectors.joining("\n")));
    }

    @Override
    public boolean isSorted() {

        return input.isSorted();
    }
}
