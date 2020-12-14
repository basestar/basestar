package io.basestar.storage.view;

import io.basestar.util.Text;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class UnionStage implements QueryStage {

    private final List<QueryStage> inputs;

    @Override
    public String toString() {

        return "- Union:\n" + inputs.stream()
                .map(v -> Text.indent(v.toString())).collect(Collectors.joining("\n"));
    }

    @Override
    public boolean isSorted() {

        return inputs.stream().allMatch(QueryStage::isSorted);
    }
}
