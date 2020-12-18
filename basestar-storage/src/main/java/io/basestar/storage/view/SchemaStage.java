package io.basestar.storage.view;

import io.basestar.schema.InstanceSchema;
import io.basestar.schema.Layout;
import io.basestar.util.Text;
import lombok.Data;

@Data
public class SchemaStage implements QueryStage {

    private final QueryStage input;

    private final InstanceSchema schema;

    @Override
    public String toString() {

        return "- Schema(schema=" + schema.getQualifiedName() + "):\n" + Text.indent(input.toString());
    }

    @Override
    public <T> T visit(final Visitor<T> visitor) {

        return visitor.visitSchema(this);
    }

    @Override
    public Layout outputLayout() {

        return schema;
    }
}
