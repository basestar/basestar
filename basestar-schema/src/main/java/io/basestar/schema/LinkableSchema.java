package io.basestar.schema;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.basestar.schema.use.Use;
import io.basestar.util.Name;

import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

public interface LinkableSchema extends InstanceSchema, Link.Resolver {

    interface Descriptor extends InstanceSchema.Descriptor {

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        Map<String, Link.Descriptor> getLinks();

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        Map<String, Permission.Descriptor> getPermissions();

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        Set<Name> getExpand();

        @Override
        LinkableSchema build(Resolver.Constructing resolver, Name qualifiedName, int slot);

        @Override
        LinkableSchema build();
    }

    Set<Name> getDeclaredExpand();

    Set<Name> getExpand();

    @Override
    default SortedMap<String, Use<?>> layout() {

        return layout(getExpand());
    }
}
