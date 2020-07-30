package io.basestar.storage.util;

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
import io.basestar.util.Nullsafe;
import io.basestar.util.PagedList;
import io.basestar.util.PagingToken;
import lombok.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

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

    public Pager(final Comparator<? super T> comparator, final List<Source<T>> sources, final PagingToken paging) {

        this(comparator, sources, EnumSet.noneOf(PagedList.Stat.class), paging);
    }

    public Pager(final Comparator<? super T> comparator, final List<Source<T>> sources, final Set<PagedList.Stat> stats, final PagingToken paging) {

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

        CompletableFuture<PagedList<T>> page(int count, PagingToken token, Set<PagedList.Stat> stats);

        default Source<T> filter(final Predicate<T> fn) {

            return (count, token, stats) -> this.page(count, token, stats).thenApply(page -> page.filter(fn));
        }

        default <U> Source<U> map(final Function<T, U> fn) {

            return (count, token, stats) -> this.page(count, token, stats).thenApply(page -> page.map(fn));
        }
    }

    public CompletableFuture<PagedList<T>> page(final int count) {

        return pageInternal(count)
                .thenApply(page -> new PagedList<>(page, encodeStates(states), stats(states)));
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

                    // FIXME: reimplement to be guaranteed stable min
                    final Optional<State<T>> first = states.stream()
                            .filter(State::hasNext)
                            .min((a, b) -> comparator.compare(a.peek(), b.peek()));

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

        private final PagingToken paging;

        private final int offset;
    }

    @Getter
    @With
    @RequiredArgsConstructor
    public static class State<T> {

        private final Source<T> source;

        private final Set<PagedList.Stat> stats;

        private final OffsetToken paging;

        private final PagedList<T> page;

        public CompletableFuture<State<T>> next(final int buffer) {

            if (this.page == null) {
                return source.page(buffer, paging.getPaging(), stats)
                        .thenCompose(page -> next(buffer, page));
            } else {
                return next(buffer, page);
            }
        }

        private CompletableFuture<State<T>> next(final int buffer, final PagedList<T> page) {

            if (paging.getOffset() >= page.size() && page.hasPaging()) {
                final PagingToken paging = page.getPaging();
                final State<T> paged = withPaging(new OffsetToken(paging, 0));
                return source.page(buffer, paging, stats)
                        .thenCompose(next -> paged.next(buffer, next));
            } else {
                return CompletableFuture.completedFuture(this.withPage(page));
            }
        }

        public boolean hasNext() {

            return !(page == null || (paging.getOffset() >= page.size() && !page.hasPaging()));
        }

        public T peek() {

            return page == null || paging.getOffset() >= page.size() ? null : page.get(paging.getOffset());
        }

        public State<T> pop() {

            return this.withPaging(paging.withOffset(paging.getOffset() + 1));
        }

        public OffsetToken paging() {

            return paging;
        }

        public CompletableFuture<State<T>> trim(final Comparator<? super T> comparator, final T value) {

            if (page != null) {
                int offset = paging.getOffset();
                while (offset < page.size() && comparator.compare(page.get(offset), value) == 0) {
                    ++offset;
                }
                if (offset != paging.getOffset()) {
                    final State<T> offsetState = this.withPaging(paging.withOffset(offset));
                    if (offset >= page.size() && page.hasPaging()) {
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
                final PagingToken token = offset.getPaging();
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

        public static <T> State<T> decode(final Source<T> source, final Set<PagedList.Stat> stats, final DataInputStream dis) throws IOException {

            final byte flag = dis.readByte();
            if (flag == 0) {
                final PagingToken token;
                final int len = dis.readShort();
                if (len > 0) {
                    final byte[] bytes = new byte[len];
                    final int read = dis.read(bytes);
                    assert (read == len);
                    token = new PagingToken(bytes);
                } else {
                    token = null;
                }
                final int offset = dis.readUnsignedShort();
                return new State<>(source, stats, new OffsetToken(token, offset), null);
            } else {
                return new State<>(source, stats, new OffsetToken(null, 0), PagedList.empty());
            }
        }

        public static <T> State<T> create(final Source<T> source, final Set<PagedList.Stat> stats) {

            return new State<>(source, stats, new OffsetToken(null, 0), null);
        }
    }

    private static <T> List<State<T>> decodeStates(final List<Source<T>> sources, final Set<PagedList.Stat> stats, final PagingToken paging) {

        if (paging == null) {
            return sources.stream().map(source -> State.create(source, stats))
                    .collect(Collectors.toList());
        }

        try (final ByteArrayInputStream bais = new ByteArrayInputStream(paging.getValue());
             final DataInputStream dis = new DataInputStream(bais)) {

            final List<State<T>> result = new ArrayList<>();

            for (final Source<T> source : sources) {
                result.add(State.decode(source, stats, dis));
            }

            return result;

        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static <T> PagingToken encodeStates(final List<State<T>> states) {

        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final DataOutputStream dos = new DataOutputStream(baos)) {

            for (final State<T> state : states) {
                state.encode(dos);
            }

            if (baos.size() == 0) {
                return null;
            } else {
                return new PagingToken(baos.toByteArray());
            }

        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static <T> PagedList.Stats stats(final List<State<T>> states) {

        return states.stream()
                .map(v -> v.page.getStats())
                .reduce(PagedList.Stats::sum)
                .orElse(PagedList.Stats.UNKNOWN);
    }
}
