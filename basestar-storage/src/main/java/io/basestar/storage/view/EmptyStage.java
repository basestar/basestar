package io.basestar.storage.view;

import io.basestar.schema.Layout;
import io.basestar.schema.LinkableSchema;
import io.basestar.util.Name;
import lombok.Data;

import java.util.Set;

@Data
public class EmptyStage implements QueryStage {

    private final LinkableSchema schema;

    private final Set<Name> expand;

    @Override
    public String toString() {

        return "- Empty(schema=" + schema.getQualifiedName() + ", expand=" + expand + ")";
    }

    @Override
    public Layout outputLayout() {

        return Layout.simple(schema.getSchema(), expand);
    }

    @Override
    public <T> T visit(final Visitor<T> visitor) {

        return visitor.visitEmpty(this);
    }
}
