package io.basestar.schema;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.basestar.util.Name;

import java.util.Map;
import java.util.Set;

public interface LinkableSchema extends InstanceSchema, Link.Resolver {

    interface Descriptor extends InstanceSchema.Descriptor {

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        Map<String, Link.Descriptor> getLinks();

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        Map<String, Permission.Descriptor> getPermissions();

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        Set<Name> getExpand();

        @Override
        LinkableSchema build(Resolver.Constructing resolver, Version version, Name qualifiedName, int slot);

        @Override
        LinkableSchema build(Name qualifiedName);

        @Override
        LinkableSchema build();
    }

    Set<Name> getDeclaredExpand();

    Set<Name> getExpand();
}
