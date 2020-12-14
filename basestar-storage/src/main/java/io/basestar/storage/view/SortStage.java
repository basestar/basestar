package io.basestar.storage.view;

import io.basestar.util.Sort;
import io.basestar.util.Text;
import lombok.RequiredArgsConstructor;

import java.util.List;

@RequiredArgsConstructor
public class SortStage implements QueryStage {

    private final QueryStage input;

    private final List<Sort> sort;

    @Override
    public String toString() {

        return "- Sort:\n" + Text.indent(input.toString());
    }

    @Override
    public boolean isSorted() {

        return true;
    }
}
