package io.basestar.storage.view;

import io.basestar.schema.Layout;
import io.basestar.util.Text;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class UnionStage implements QueryStage {

    private final List<QueryStage> inputs;

    private final Layout outputLayout;

    @Override
    public String toString() {

        return "- Union:\n" + inputs.stream()
                .map(v -> Text.indent(v.toString()))
                .collect(Collectors.joining("\n"));
    }

    @Override
    public Layout outputLayout() {

        return outputLayout;
    }

    @Override
    public <T> T visit(final Visitor<T> visitor) {

        return visitor.visitUnion(this);
    }
}
