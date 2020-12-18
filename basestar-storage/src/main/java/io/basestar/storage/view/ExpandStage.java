package io.basestar.storage.view;

import io.basestar.schema.Layout;
import io.basestar.schema.LinkableSchema;
import io.basestar.util.Name;
import io.basestar.util.Text;
import lombok.Data;

import java.util.Set;

@Data
public class ExpandStage implements QueryStage {

    private final QueryStage input;

    private final LinkableSchema schema;

    private final Set<Name> expand;

    @Override
    public String toString() {

        return "- Expand(schema=" + schema.getQualifiedName() + ", expand=" + expand + ")\n"
                + Text.indent(input.toString());
    }

    @Override
    public Layout outputLayout() {

        return Layout.simple(schema.getSchema(), expand);
    }

    @Override
    public <T> T visit(final Visitor<T> visitor) {

        return visitor.visitExpand(this);
    }
}
