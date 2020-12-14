package io.basestar.storage.view;

import io.basestar.schema.LinkableSchema;
import lombok.Data;

@Data
public class SourceStage implements QueryStage {

    private final LinkableSchema schema;

    @Override
    public String toString() {

        return "- Source: " + schema.getQualifiedName();
    }

    @Override
    public boolean isSorted() {

        return false;
    }
}
