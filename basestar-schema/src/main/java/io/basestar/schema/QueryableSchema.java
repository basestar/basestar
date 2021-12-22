package io.basestar.schema;

import io.basestar.expression.Context;
import io.basestar.util.Name;
import io.basestar.util.Sort;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public interface QueryableSchema extends InstanceSchema, Link.Resolver, Permission.Resolver {

    List<Sort> sort();

    default List<Sort> sort(final List<Sort> sort) {

        final List<Sort> result = new ArrayList<>(sort);
        sort().forEach(s -> {
            if (sort.stream().noneMatch(s2 -> s2.getName().equals(s.getName()))) {
                result.add(s);
            }
        });
        return result;
    }

    interface Descriptor<S extends QueryableSchema> extends InstanceSchema.Descriptor<S>, Link.Resolver.Descriptor, Permission.Resolver.Descriptor {

        interface Self<S extends QueryableSchema> extends InstanceSchema.Descriptor.Self<S>, Descriptor<S> {

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

    interface Builder<B extends Builder<B, S>, S extends QueryableSchema> extends InstanceSchema.Builder<B, S>, Descriptor<S>, Link.Resolver.Builder<B>, Permission.Resolver.Builder<B> {

    }

    @Override
    Descriptor<? extends QueryableSchema> descriptor();

    @Override
    default Set<Constraint.Violation> validate(final Context context, final Name name, final Instance after) {

        return validate(context, name, after, after);
    }

    default Set<Constraint.Violation> validate(final Context context, final Instance before, final Instance after) {

        return validate(context, Name.empty(), before, after);
    }

    default Set<Constraint.Violation> validate(final Context context, final Name name, final Instance before, final Instance after) {

        return this.getProperties().values().stream()
                .flatMap(v -> v.validate(context, name, before.get(v.getName()), after.get(v.getName())).stream())
                .collect(Collectors.toSet());
    }
}
