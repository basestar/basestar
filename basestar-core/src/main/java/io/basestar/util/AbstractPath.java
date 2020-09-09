package io.basestar.util;

/*-
 * #%L
 * basestar-core
 * %%
 * Copyright (C) 2019 - 2020 Basestar.IO
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.google.common.base.Joiner;
import com.google.common.collect.*;
import lombok.Data;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Data
public abstract class AbstractPath<SELF extends AbstractPath<SELF>> implements Iterable<String>, Comparable<SELF>, Serializable {

    public static final String SELF = ".";

    public static final String UP = "..";

    private final List<String> parts;

    protected AbstractPath(final String ... parts) {

        this.parts = ImmutableList.copyOf(parts);
    }

    protected AbstractPath(final Iterable<String> parts) {

        this.parts = ImmutableList.copyOf(parts);
    }

    public Stream<String> stream() {

        return Streams.stream(this);
    }

    protected abstract char delimiter();

    protected abstract SELF create();

    protected abstract SELF create(final List<String> parts);

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

    public SELF withFirst() {

        return parts.isEmpty() ? self() : create(parts.subList(0, 1));
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

    public SELF withLast() {

        return parts.isEmpty() ? self() : create(parts.subList(parts.size() - 2, parts.size() - 1));
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

    public SELF range(final int from) {

        return range(from, this.parts.size());
    }

    public SELF range(final int from, final int to) {

        return create(this.parts.subList(from, to));
    }

    @Override
    public String toString() {

        return joiner(delimiter()).join(parts);
    }

    public String toString(final char delimiter) {

        return joiner(delimiter).join(parts);
    }

    public String toString(final String delimiter) {

        return joiner(delimiter).join(parts);
    }

    @SuppressWarnings("unchecked")
    public Object apply(final Map<String, Object> data) {

        final Object target = data.get(first());
        final AbstractPath<SELF> tail = withoutFirst();
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
                results.add(path.range(parent.size()));
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

    protected static Joiner joiner(final char delimiter) {

        return Joiner.on(delimiter);
    }

    protected static Joiner joiner(final String delimiter) {

        return Joiner.on(delimiter);
    }

    public SELF transform(final Function<String, String> fn) {

        return create(parts.stream().map(fn).collect(Collectors.toList()));
    }

    public SELF relative(final SELF other) {

        final int thisSize = size();
        final int otherSize = other.size();
        int i = 0;
        while(i < thisSize && i < otherSize) {
            final String part = get(i);
            if(part.equals(other.get(i))) {
                ++i;
            } else {
                break;
            }
        }
        final List<String> parts = new ArrayList<>();
        for(int j = i; j != thisSize; ++j) {
            parts.add("..");
        }
        for(int j = i; j != otherSize; ++j) {
            parts.add(other.get(j));
        }
        return create(parts);
    }

    public SELF canonical() {

        final LinkedList<String> parts = new LinkedList<>();
        for(final String part : this.parts) {
            if(part.equals(UP)) {
                if(parts.isEmpty()) {
                    parts.add(UP);
                } else {
                    parts.pop();
                }
            } else if(!part.equals(SELF)) {
                parts.add(part);
            }
        }
        return create(parts);
    }

    public SELF up(final int count) {

        final List<String> parts = Lists.newArrayList(this.parts);
        for(int i = 0; i != count; ++i) {
            parts.add(UP);
        }
        return create(parts);
    }

    public SELF toLowerCase() {

        return transform(String::toLowerCase);
    }

    public SELF toUpperCase() {

        return transform(String::toUpperCase);
    }

    public String[] toArray() {

        return parts.toArray(new String[0]);
    }
}
