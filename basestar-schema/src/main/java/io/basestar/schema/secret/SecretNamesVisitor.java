package io.basestar.schema.secret;

import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseContainer;
import io.basestar.schema.use.UseInstance;
import io.basestar.schema.use.UseMap;
import io.basestar.util.Name;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;


public class SecretNamesVisitor implements Use.Visitor.Defaulting<Set<Name>> {

    public <T> Set<Name> visitDefault(final Use<T> type) {

        return Collections.emptySet();
    }

    @Override
    public <V, T> Set<Name> visitContainer(final UseContainer<V, T> type) {

        return visit(type.getType());
    }

    @Override
    public Set<Name> visitInstance(final UseInstance type) {

        final Set<Name> result = new HashSet<>();

        return null;
    }

    @Override
    public <T> Set<Name> visitMap(final UseMap<T> type) {

        return null;
    }
}
