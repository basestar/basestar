package io.basestar.schema;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.collect.ImmutableMap;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface LinkableSchema extends InstanceSchema, Link.Resolver, Permission.Resolver {

    interface Descriptor<S extends LinkableSchema> extends InstanceSchema.Descriptor<S>, Link.Resolver.Descriptor, Permission.Resolver.Descriptor {

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        Set<Name> getExpand();

        interface Self<S extends LinkableSchema> extends InstanceSchema.Descriptor.Self<S>, Descriptor<S> {

            @Override
            default Set<Name> getExpand() {

                return self().getDeclaredExpand();
            }

            @Override
            default Map<String, Link.Descriptor> getLinks() {

                return self().describeDeclaredLinks();
            }

            @Override
            default Map<String, Permission.Descriptor> getPermissions() {

                return self().describeDeclaredPermissions();
            }
        }
    }

    interface Builder<B extends Builder<B, S>, S extends LinkableSchema> extends InstanceSchema.Builder<B, S>, Descriptor<S>, Link.Resolver.Builder<B>, Permission.Resolver.Builder<B> {

    }

    static SortedSet<Name> extendExpand(final List<? extends InstanceSchema> base, final Set<Name> extend) {

        return Immutable.sortedCopy(Stream.concat(
                base.stream().flatMap(schema -> schema.getExpand().stream()),
                Nullsafe.orDefault(extend).stream()
        ).collect(Collectors.toSet()));
    }

    Set<Name> getDeclaredExpand();

    Set<Name> getExpand();

    @Override
    Descriptor<? extends LinkableSchema> descriptor();

    default Instance deleted(final String id) {

        return new Instance(ImmutableMap.of(
                id(), id,
                Reserved.DELETED, true
        ));
    }
}
