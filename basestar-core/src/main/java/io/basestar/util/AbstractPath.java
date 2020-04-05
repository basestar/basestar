package io.basestar.util;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import lombok.Data;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Data
public abstract class AbstractPath<SELF extends AbstractPath<SELF>> implements Iterable<String>, Comparable<SELF>, Serializable {

    private final List<String> parts;

    public AbstractPath(final String ... parts) {

        this.parts = ImmutableList.copyOf(parts);
    }

    public AbstractPath(final List<String> parts) {

        this.parts = ImmutableList.copyOf(parts);
    }

    public Stream<String> stream() {

        return StreamSupport.stream(this.spliterator(), false);
    }

    protected abstract char delimiter();

    protected abstract SELF create();

    protected abstract SELF create(final List<String> parts);

//
////    @JsonCreator
//    public static AbstractPath parse(final String path) {
//
//        return new AbstractPath(splitter().splitToList(path));
//    }
//
//    public static AbstractPath parse(final String path, final String delimiter) {
//
//        return new AbstractPath(splitter(delimiter).splitToList(path));
//    }
//
//    public static Set<AbstractPath> parseSet(final String paths) {
//
//        return MULTIPLE_SPLITTER.splitToList(paths).stream().map(AbstractPath::parse)
//                .collect(Collectors.toSet());
//    }
//
//    public static List<AbstractPath> parseList(final String paths) {
//
//        return MULTIPLE_SPLITTER.splitToList(paths).stream().map(AbstractPath::parse)
//                .collect(Collectors.toList());
//    }
//
//    public static AbstractPath of(final String ... parts) {
//
//        return new AbstractPath(Arrays.asList(parts));
//    }
//
//    public static AbstractPath of(final List<String> parts) {
//
//        return new AbstractPath(parts);
//    }
//
//    public static AbstractPath empty() {
//
//        return new AbstractPath(Collections.emptyList());
//    }

    public boolean isEmpty() {

        return parts.isEmpty();
    }

    public String get(final int i) {

        return parts.get(i);
    }

    public String first() {

        if(parts.isEmpty()) {
            return null;
        } else {
            return parts.get(0);
        }
    }

    public SELF withoutFirst() {

        if(parts.isEmpty() || parts.size() == 1) {
            return create();
        } else {
            return create(parts.subList(1, parts.size()));
        }
    }

    public SELF withoutFirst(final int size) {

        if(parts.isEmpty() || parts.size() < size) {
            return create();
        } else {
            return create(parts.subList(size, parts.size()));
        }
    }

    public String last() {

        if(parts.isEmpty()) {
            return null;
        } else {
            return parts.get(parts.size() - 1);
        }
    }

    public SELF withoutLast() {

        if(parts.isEmpty() || parts.size() == 1) {
            return create();
        } else {
            return create(parts.subList(0, parts.size() - 1));
        }
    }

    public SELF with(final SELF tail) {

        return with(tail.getParts());
    }

    public SELF with(final List<String> parts) {

        final List<String> merged = new ArrayList<>(this.parts);
        merged.addAll(parts);
        return create(merged);
    }

    public SELF with(final String ... parts) {

        return with(Arrays.asList(parts));
    }

    public int size() {

        return this.parts.size();
    }

    public SELF subPath(final int from) {

        return subPath(from, this.parts.size());
    }

    public SELF subPath(final int from, final int to) {

        return create(this.parts.subList(from, to));
    }

    @Override
    public String toString() {

        return joiner(delimiter()).join(parts);
    }

    public String toString(final String delimiter) {

        return joiner(delimiter).join(parts);
    }

    @SuppressWarnings("unchecked")
    public Object apply(final Map<String, Object> data) {

        final Object target = data.get(first());
        final AbstractPath tail = withoutFirst();
        if(tail.isEmpty()) {
            return target;
        } else if(target instanceof Map) {
            return tail.apply((Map<String, Object>)target);
        } else {
            return null;
        }
    }

    @Nonnull
    public static <SELF extends AbstractPath<SELF>> Map<String, Set<SELF>> branch(@Nullable final Collection<SELF> paths) {

        if(paths == null) {
            return Collections.emptyMap();
        }
        final Multimap<String, SELF> results = HashMultimap.create();
        for(final SELF path : paths) {
            if(!path.isEmpty()) {
                final String head = path.first();
                final SELF tail = path.withoutFirst();
                results.put(head, tail);
            }
        }

        return results.asMap().entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> ImmutableSet.copyOf(entry.getValue())
                ));
    }

    public static <T extends AbstractPath<T>> Set<T> simplify(final Set<T> paths) {

        final Set<T> results = new HashSet<>();
        paths.stream().sorted(Comparator.comparing(AbstractPath<T>::size).reversed())
                .forEach(parent -> {
                    if(results.stream().noneMatch(parent::isParentOrEqual)) {
                        results.add(parent);
                    }
                });
        return results;
    }

    public boolean isParentOrEqual(final SELF of) {

        if(this.parts.size() <= of.size()) {
            for(int i = 0; i != this.parts.size(); ++i) {
                if(!of.getParts().get(i).equals(parts.get(i))) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    public boolean isParent(final SELF of) {

        if(this.parts.size() < of.size()) {
            return isParentOrEqual(of);
        } else {
            return false;
        }
    }

    public boolean isChild(final SELF of) {

        return of.isParent(self());
    }

    public boolean isChildOrEqual(final SELF of) {

        return of.isParentOrEqual(self());
    }

    @SuppressWarnings("unchecked")
    private SELF self() {

        return (SELF)this;
    }

    public static <SELF extends AbstractPath<SELF>> Set<SELF> children(final Collection<SELF> paths, final String parent) {

        final Set<SELF> results = new HashSet<>();
        for(final SELF path : paths) {
            if(!path.isEmpty() && path.first().equals(parent)) {
                final SELF tail = path.withoutFirst();
                if(!tail.isEmpty()) {
                    results.add(tail);
                }
            }
        }
        return results;
    }

    public static <SELF extends AbstractPath<SELF>> Set<SELF> children(final Collection<SELF> paths, final SELF parent) {

        final Set<SELF> results = new HashSet<>();
        for(final SELF path : paths) {
            if(parent.isParentOrEqual(path)) {
                results.add(path.subPath(parent.size()));
            }
        }
        return results;
    }

    @Nonnull
    @Override
    public Iterator<String> iterator() {

        return parts.iterator();
    }

    @Override
    public int compareTo(@Nonnull final SELF other) {

        return toString().compareTo(other.toString());
    }

    protected static Splitter splitter(final char delimiter) {

        return Splitter.on(delimiter).omitEmptyStrings().trimResults();
    }

    protected static Joiner joiner(final char delimiter) {

        return Joiner.on(delimiter);
    }

    protected static Joiner joiner(final String delimiter) {

        return Joiner.on(delimiter);
    }
}
