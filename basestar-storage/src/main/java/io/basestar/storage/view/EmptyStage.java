package io.basestar.storage.view;

public class EmptyStage implements QueryStage {

    @Override
    public String toString() {

        return "- Empty";
    }

    @Override
    public boolean isSorted() {

        return true;
    }
}
