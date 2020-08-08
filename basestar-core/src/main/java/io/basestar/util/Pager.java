package io.basestar.util;

/*-
 * #%L
 * basestar-storage
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

import com.google.common.collect.ImmutableList;
import lombok.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Assumes inputs are already sorted.
 *
 * Also de-duplicates (where comparator(a, b) == 0).
 *
 * Caller(s) must keep sources in same order.
 *
 * @param <T>
 */

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class Pager<T> {

    // Never read less than this count per source, prevents lots of tiny reads that may otherwise result in trims
    private static final int DEFAULT_BUFFER = 10;

    // Aggressive trimming
    private static final int TRIM_BUFFER = DEFAULT_BUFFER;

    private final Comparator<? super T> comparator;

    private List<State<T>> states;

    private Page.Stats stats;

    public Pager(final Comparator<? super T> comparator, final List<Source<T>> sources, final Page.Token paging) {

        this(comparator, sources, EnumSet.noneOf(Page.Stat.class), paging);
    }

    public Pager(final Comparator<? super T> comparator, final List<Source<T>> sources, final Set<Page.Stat> stats, final Page.Token paging) {

        this.comparator = Nullsafe.require(comparator);
        this.states = decodeStates(sources, EnumSet.copyOf(stats), paging);
    }

    public static <T> List<Source<T>> filter(final List<Source<T>> sources, final Predicate<T> fn) {

        return sources.stream().map(source -> source.filter(fn)).collect(Collectors.toList());
    }

    public static <T, U> List<Source<U>> map(final List<Source<T>> sources, final Function<T, U> fn) {

        return sources.stream().map(source -> source.map(fn)).collect(Collectors.toList());
    }

    public interface Source<T> {

        CompletableFuture<Page<T>> page(int count, Page.Token token, Set<Page.Stat> stats);

        default Source<T> filter(final Predicate<T> fn) {

            return (count, token, stats) -> this.page(count, token, stats).thenApply(page -> page.filter(fn));
        }

        default <U> Source<U> map(final Function<T, U> fn) {

            return (count, token, stats) -> this.page(count, token, stats).thenApply(page -> page.map(fn));
        }

        static <T> Source<T> empty() {

            return (count, token, stats) -> CompletableFuture.completedFuture(Page.empty());
        }
    }

//    @Data
//    private static class Item<T> {
//
//        private final int source;
//
//        private final int position;
//
//        private final T value;
//    }

    public CompletableFuture<Page<T>> page(final int count) {

        return pageInternal(count)
                .thenApply(page -> new Page<>(page, encodeStates(states), stats));
    }

    private CompletableFuture<List<T>> pageInternal(final int count) {

        final int buffer = Math.max(count, DEFAULT_BUFFER);
        if (count == 0) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        } else if (count == 1) {
            return next(buffer).thenApply(head -> {
                if (head == null) {
                    return Collections.emptyList();
                } else {
                    return Collections.singletonList(head);
                }
            });
        } else {
            return next(buffer).thenCompose(head -> {
                if (head == null) {
                    return CompletableFuture.completedFuture(Collections.emptyList());
                } else {
                    return pageInternal(count - 1)
                            .thenApply(rest -> ImmutableList.<T>builder()
                                    .add(head)
                                    .addAll(rest)
                                    .build());
                }
            });
        }
    }

    private CompletableFuture<T> next(final int buffer) {

        final List<CompletableFuture<State<T>>> futures = states.stream()
                .map(state -> state.next(buffer))
                .collect(Collectors.toList());

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]))
                .thenCompose(ignored -> {

                    final List<State<T>> states = futures.stream()
                            .map(v -> v.getNow(null))
                            .collect(Collectors.toList());

                    if(stats == null) {
                        stats = states.stream().map(State::getPage).filter(Objects::nonNull)
                                .map(Page::getStats).map(v -> v != null ? v : Page.Stats.NULL)
                                .reduce(Page.Stats::sum).orElse(Page.Stats.NULL);
                    }

                    final Stream<State<T>> next = states.stream().filter(State::hasNext);

                    // FIXME: reimplement to be guaranteed stable min
                    final Optional<State<T>> first = comparator == null ? next.findFirst()
                            : next.min((a, b) -> comparator.compare(a.peek(), b.peek()));

                    if (first.isPresent()) {

                        final T result = first.get().peek();

                        final List<CompletableFuture<State<T>>> trimFutures = states.stream()
                                .map(v -> v.trim(comparator, result))
                                .collect(Collectors.toList());

                        return CompletableFuture.allOf(trimFutures.toArray(new CompletableFuture<?>[0]))
                                .thenApply(ignored2 -> {

                                    this.states = trimFutures.stream()
                                            .map(v -> v.getNow(null))
                                            .collect(Collectors.toList());

                                    return result;
                                });

                    } else {

                        this.states = Collections.emptyList();
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    @Data
    @With
    private static class OffsetToken {

        private final Page.Token paging;

        private final int offset;
    }

    @Getter
    @RequiredArgsConstructor
    public static class State<T> {

        private final Source<T> source;

        private final Set<Page.Stat> stats;

        @With
        private final OffsetToken paging;

        @With
        private final Page<T> page;

        public CompletableFuture<State<T>> next(final int buffer) {

            if (this.page == null) {
                return source.page(buffer, paging.getPaging(), stats)
                        .thenCompose(page -> next(buffer, page));
            } else {
                return next(buffer, page);
            }
        }

        private CompletableFuture<State<T>> next(final int buffer, final Page<T> page) {

            if (paging.getOffset() >= page.size() && page.hasMore()) {
                final Page.Token paging = page.getPaging();
                final State<T> paged = withPaging(new OffsetToken(paging, 0));
                return source.page(buffer, paging, stats)
                        .thenCompose(next -> paged.next(buffer, next));
            } else {
                return CompletableFuture.completedFuture(this.withPage(page));
            }
        }

        public boolean hasNext() {

            return !(page == null || (paging.getOffset() >= page.size() && !page.hasMore()));
        }

        public T peek() {

            return page == null || paging.getOffset() >= page.size() ? null : page.get(paging.getOffset());
        }

//        public State<T> pop() {
//
//            return this.withPaging(paging.withOffset(paging.getOffset() + 1));
//        }

        public CompletableFuture<State<T>> trim(final Comparator<? super T> comparator, final T value) {

            if (page != null && comparator != null) {
                int offset = paging.getOffset();
                while (offset < page.size() && comparator.compare(page.get(offset), value) == 0) {
                    ++offset;
                }
                if (offset != paging.getOffset()) {
                    final State<T> offsetState = this.withPaging(paging.withOffset(offset));
                    if (offset >= page.size() && page.hasMore()) {
                        return offsetState.next(TRIM_BUFFER).thenCompose(next -> next.trim(comparator, value));
                    } else {
                        return CompletableFuture.completedFuture(offsetState);
                    }
                }
            }
            return CompletableFuture.completedFuture(this);
        }

        public void encode(final DataOutputStream dos) throws IOException {

            if (hasNext()) {
                dos.writeByte(0);
                final OffsetToken offset = getPaging();
                final Page.Token token = offset.getPaging();
                if (token == null) {
                    dos.writeShort(0);
                } else {
                    final byte[] bytes = token.getValue();
                    dos.writeShort(bytes.length);
                    dos.write(bytes);
                }
                dos.writeShort(offset.getOffset());
            } else {
                dos.writeByte(1);
            }
        }

        public static <T> State<T> decode(final Source<T> source, final Set<Page.Stat> stats, final DataInputStream dis) throws IOException {

            final byte flag = dis.readByte();
            if (flag == 0) {
                final Page.Token token;
                final int len = dis.readShort();
                if (len > 0) {
                    final byte[] bytes = new byte[len];
                    final int read = dis.read(bytes);
                    assert (read == len);
                    token = new Page.Token(bytes);
                } else {
                    token = null;
                }
                final int offset = dis.readUnsignedShort();
                return new State<>(source, stats, new OffsetToken(token, offset), null);
            } else {
                return new State<>(source, stats, new OffsetToken(null, 0), Page.empty());
            }
        }

        public static <T> State<T> create(final Source<T> source, final Set<Page.Stat> stats) {

            return new State<>(source, stats, new OffsetToken(null, 0), null);
        }
    }

    private static <T> List<State<T>> decodeStates(final List<Source<T>> sources, final Set<Page.Stat> stats, final Page.Token paging) {

        final List<State<T>> result = new ArrayList<>();

        if (paging == null) {

            sources.forEach(source -> result.add(State.create(source, stats)));

        } else {

            try (final ByteArrayInputStream bais = new ByteArrayInputStream(paging.getValue());
                 final DataInputStream dis = new DataInputStream(bais)) {

                for (final Source<T> source : sources) {
                    result.add(State.decode(source, stats, dis));
                }

            } catch (final IOException e) {
                throw new IllegalStateException(e);
            }
        }

        return result;
    }

    private static <T> Page.Token encodeStates(final List<State<T>> states) {

        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final DataOutputStream dos = new DataOutputStream(baos)) {

            for (final State<T> state : states) {
                state.encode(dos);
            }

            if (baos.size() == 0) {
                return null;
            } else {
                return new Page.Token(baos.toByteArray());
            }

        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
