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
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.With;

import java.io.*;
import java.nio.charset.StandardCharsets;
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

public interface Pager<T> {

    CompletableFuture<Page<T>> page(Set<Page.Stat> stats, Page.Token token, int count);

    static <T> Pager<T> empty() {

        return (stats, token, count) -> CompletableFuture.completedFuture(Page.empty());
    }

    static <T> Pager<T> simple(final List<? extends T> list) {

        return simple(CompletableFuture.completedFuture(list));
    }

    static <T> Pager<T> simple(final CompletableFuture<? extends List<? extends T>> future) {

        return (stats, token, count) -> future.thenApply(list -> {
            final long offset;
            if (token != null) {
                offset = token.getLongValue();
            } else {
                offset = 0;
            }
            if (offset < list.size()) {
                final int end = Math.min((int) (offset + count), list.size());
                final List<T> page = Immutable.list(list.subList((int) offset, end));
                final Page.Token newToken = end == list.size() ? null : Page.Token.fromLongValue((long) end);
                return new Page<>(page, newToken, token == null ? Page.Stats.fromTotal(list.size()) : null);
            } else {
                return new Page<>(ImmutableList.of(), null);
            }
        });
    }
    
    default CompletableFuture<Page<T>> page(final Page.Token token, final int count) {

        return page(Collections.emptySet(), token, count);
    }

    default CompletableFuture<Page<T>> page(final int count) {

        return page(Collections.emptySet(), null, count);
    }

    default Pager<T> filter(final Predicate<T> fn) {

        return (stats, token, count) -> page(stats, token, count).thenApply(page -> page.filter(fn));
    }

    default <U> Pager<U> map(final Function<T, U> fn) {

        return (stats, token, count) -> page(stats, token, count).thenApply(page -> page.map(fn));
    }

    static <T> Pager<T> merge(final Comparator<? super T> comparator, final Map<String, Pager<T>> pagers) {

        return (stats, token, count) -> new Merge<>(comparator, pagers, stats, token).page(count);
    }

    class Merge<T> {

        // Never read less than this count per source, prevents lots of tiny reads that may otherwise result in trims
        private static final int DEFAULT_BUFFER = 10;

        // Aggressive trimming
        private static final int TRIM_BUFFER = DEFAULT_BUFFER;

        private final Comparator<? super T> comparator;

        private List<State<T>> states;

        private Page.Stats stats;

        public Merge(final Comparator<? super T> comparator, final Map<String, Pager<T>> sources, final Page.Token paging) {

            this(comparator, sources, EnumSet.noneOf(Page.Stat.class), paging);
        }

        public Merge(final Comparator<? super T> comparator, final Map<String, Pager<T>> sources, final Set<Page.Stat> stats, final Page.Token paging) {

            this.comparator = Nullsafe.require(comparator);
            this.states = decodeStates(sources, stats.isEmpty() ? EnumSet.noneOf(Page.Stat.class) : EnumSet.copyOf(stats), paging);
        }

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

                        if (stats == null) {
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

            private final String id;

            private final Pager<T> source;

            private final Set<Page.Stat> stats;

            @With
            private final OffsetToken paging;

            @With
            private final Page<T> page;

            public CompletableFuture<State<T>> next(final int buffer) {

                if (this.page == null) {
                    return source.page(stats, paging.getPaging(), buffer)
                            .thenCompose(page -> next(buffer, page));
                } else {
                    return next(buffer, page);
                }
            }

            private CompletableFuture<State<T>> next(final int buffer, final Page<T> page) {

                if (paging.getOffset() >= page.size() && page.hasMore()) {
                    final Page.Token paging = page.getPaging();
                    final State<T> paged = withPaging(new OffsetToken(paging, 0));
                    return source.page(stats, paging, buffer)
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

            public static <T> State<T> decode(final String id, final Pager<T> source, final Set<Page.Stat> stats, final DataInputStream dis) throws IOException {

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
                    return new State<>(id, source, stats, new OffsetToken(token, offset), null);
                } else {
                    return new State<>(id, source, stats, new OffsetToken(null, 0), Page.empty());
                }
            }

            public static <T> State<T> create(final String id, final Pager<T> source, final Set<Page.Stat> stats) {

                return new State<>(id, source, stats, new OffsetToken(null, 0), null);
            }
        }

        private static <T> List<State<T>> decodeStates(final Map<String, Pager<T>> sources, final Set<Page.Stat> stats, final Page.Token paging) {

            final List<State<T>> result = new ArrayList<>();

            if (paging == null) {

                sources.forEach((id, source) -> result.add(State.create(id, source, stats)));

            } else {

                try (final ByteArrayInputStream bais = new ByteArrayInputStream(paging.getValue());
                     final DataInputStream dis = new DataInputStream(bais)) {

                    final Set<String> seenKeys = new HashSet<>();
                    while(true) {
                        final char keyLength = dis.readChar();
                        if(keyLength == 0) {
                            break;
                        } else {
                            final byte[] keyBytes = new byte[keyLength];
                            dis.readFully(keyBytes);
                            final String key = new String(keyBytes, StandardCharsets.UTF_8);
                            final Pager<T> source = sources.get(key);
                            if(source == null) {
                                throw new IllegalStateException("Page token not recognised: " + key);
                            } else {
                                seenKeys.add(key);
                                result.add(State.decode(key, source, stats, dis));
                            }
                        }
                    }
                    if(!seenKeys.equals(sources.keySet())) {
                        throw new IllegalStateException("Page tokens not matched: " + seenKeys + "!=" + sources.keySet());
                    }

                } catch (final IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            return result;
        }

        private static <T> Page.Token encodeStates(final List<State<T>> states) {

            if(states.isEmpty()) {
                return null;
            }
            try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                 final DataOutputStream dos = new DataOutputStream(baos)) {

                for (final State<T> state : states) {
                    final byte[] keyBytes = state.getId().getBytes(StandardCharsets.UTF_8);
                    final int keyLength = keyBytes.length;
                    if (keyLength > Character.MAX_VALUE) {
                        throw new IllegalStateException("Page token key too long");
                    }
                    dos.writeChar(keyLength);
                    dos.write(keyBytes);
                    state.encode(dos);
                }
                dos.writeChar(0);

                return new Page.Token(baos.toByteArray());

            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
