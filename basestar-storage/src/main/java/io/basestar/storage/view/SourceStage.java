package io.basestar.storage.view;

import io.basestar.schema.Layout;
import io.basestar.schema.LinkableSchema;
import lombok.Data;

@Data
public class SourceStage implements QueryStage {

    private final LinkableSchema schema;

    @Override
    public String toString() {

        return "- Source(schema=" + schema.getQualifiedName() + ")";
    }

    @Override
    public Layout outputLayout() {

        return schema;
    }

    @Override
    public <T> T visit(final Visitor<T> visitor) {

        return visitor.visitSource(this);
    }
}
