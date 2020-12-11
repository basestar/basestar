package io.basestar.storage.view;

import io.basestar.expression.Expression;
import io.basestar.schema.LinkableSchema;
import io.basestar.util.Name;
import lombok.Data;

import java.util.Map;
import java.util.Set;

@Data
public class LinkStage implements ViewStage {

    private final Map<String, Join> joins;

    @Data
    public static class Join {

        private final Expression condition;

        private final LinkableSchema schema;

        private final Set<Name> expand;

        private final boolean single;
    }
}
