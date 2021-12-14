package io.basestar.schema;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.basestar.expression.Context;
import io.basestar.jackson.serde.AbbrevListDeserializer;
import io.basestar.schema.use.Use;
import io.basestar.util.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface LinkableSchema extends InstanceSchema, Link.Resolver, Query.Resolver, Permission.Resolver {

    String __ID = "__id";

    default List<Sort> sort(final List<Sort> sort) {

        final List<Sort> result = new ArrayList<>(sort);
        sort().forEach(s -> {
            if (sort.stream().noneMatch(s2 -> s2.getName().equals(s.getName()))) {
                result.add(s);
            }
        });
        return result;
    }

    List<Sort> sort();

    interface Descriptor<S extends LinkableSchema> extends InstanceSchema.Descriptor<S>, Link.Resolver.Descriptor, Query.Resolver.Descriptor, Permission.Resolver.Descriptor {

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        @JsonDeserialize(using = AbbrevListDeserializer.class)
        Set<Name> getExpand();

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        @JsonDeserialize(using = AbbrevListDeserializer.class)
        List<Bucketing> getBucket();

        interface Self<S extends LinkableSchema> extends InstanceSchema.Descriptor.Self<S>, Descriptor<S> {

            @Override
            default Set<Name> getExpand() {

                return self().getDeclaredExpand();
            }

            default List<Bucketing> getBucket() {

                return self().getDeclaredBucketing();
            }

            @Override
            default Map<String, Link.Descriptor> getLinks() {

                return self().describeDeclaredLinks();
            }

            @Override
            default Map<String, Permission.Descriptor> getPermissions() {

                return self().describeDeclaredPermissions();
            }

            @Override
            default Map<String, Query.Descriptor> getQueries() {

                return self().describeDeclaredQueries();
            }
        }
    }

    interface Builder<B extends Builder<B, S>, S extends LinkableSchema> extends InstanceSchema.Builder<B, S>, Descriptor<S>, Link.Resolver.Builder<B>, Query.Resolver.Builder<B>, Permission.Resolver.Builder<B> {

        B setExpand(Set<Name> expand);

        B setBucket(List<Bucketing> bucket);
    }

    static SortedSet<Name> extendExpand(final List<? extends InstanceSchema> base, final Set<Name> extend) {

        return Immutable.sortedSet(Stream.concat(
                base.stream().flatMap(schema -> schema.getExpand().stream()),
                Nullsafe.orDefault(extend).stream()
        ).collect(Collectors.toSet()));
    }

    Set<Name> getDeclaredExpand();

    List<Bucketing> getDeclaredBucketing();

    default List<Bucketing> getEffectiveBucketing() {

        final List<Bucketing> bucketing = getDeclaredBucketing();
        if(bucketing.isEmpty()) {
            return ImmutableList.of(
                    new Bucketing(ImmutableList.of(Name.of(id())))
            );
        } else {
            return bucketing;
        }
    }

    @Override
    Descriptor<? extends LinkableSchema> descriptor();

    String id();

    String id(Map<String, Object> data);

    /**
     * For treating a view record as an object type record regardless of primary key configuration,
     * temporary measure until view-id considerations are resolved
     */

    String forceId(Map<String, Object> data);

    @SuppressWarnings(Warnings.RETURN_GENERIC_WILDCARD)
    Use<?> typeOfId();

    default Instance deleted(final String id) {

        return new Instance(ImmutableMap.of(
                id(), id,
                Reserved.DELETED, true
        ));
    }

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

    default boolean isCompatibleBucketing(final List<Bucketing> other) {

        final List<Bucketing> effective = getEffectiveBucketing();
        if(effective.size() == other.size()) {
            for(int i = 0; i != effective.size(); ++i) {
                if(!effective.get(i).isCompatible(other.get(i))) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    @Override
    default boolean isCompatibleBucketing(final List<Bucketing> other, final Name name) {

        if(name.isEmpty()) {
            return isCompatibleBucketing(other);
        }
        final Member member = requireMember(name.first(), true);
        return member.isCompatibleBucketing(other, name.withoutFirst());
    }
}
