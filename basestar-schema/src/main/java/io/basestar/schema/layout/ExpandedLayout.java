package io.basestar.schema.layout;

import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseObject;
import io.basestar.schema.use.UseStruct;
import io.basestar.util.Name;
import lombok.Getter;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Converts expanded objects to structs
 */

public class ExpandedLayout implements Layout.Simple {

    @Getter
    private final Layout baseLayout;

    public ExpandedLayout(final Layout baseLayout) {

        this.baseLayout = baseLayout;
    }

    @Override
    public Use<?> layoutSchema(final Use<?> type, final Set<Name> expand) {

        return type.visit(new Use.Visitor.Transforming() {

            @Override
            public Set<Name> getExpand() {

                return expand;
            }

            @Override
            public Use<?> transform(final Use<?> type, final Set<Name> expand) {

                return layoutSchema(type, expand);
            }

            @Override
            public Use<?> visitObject(final UseObject type) {

                if(expand != null) {
                    final Map<String, Use<?>> schema = type.getSchema().layoutSchema(expand);
                    final Map<String, Set<Name>> branches = Name.branch(expand);
                    return UseStruct.from(schema.entrySet().stream().collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> layoutSchema(e.getValue(), branches.get(e.getKey()))
                    )));
                } else {
                    return type;
                }
            }
        });
    }

    // Does not change representation, just schema

    @Override
    public Object applyLayout(final Use<?> type, final Set<Name> expand, final Object value) {

        return value;
    }

    // Does not change representation, just schema

    @Override
    public Object unapplyLayout(final Use<?> type, final Set<Name> expand, final Object value) {

        return value;
    }
}
