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

import com.google.common.base.Charsets;
import com.google.common.io.BaseEncoding;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.With;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Data
@With
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class Page<T> extends AbstractList<T> implements Serializable {

    public static <T> Page<T> merge(final Map<String, Page<T>> pages, final Comparator<T> comparator, final int count) {

//        final List<T> results = new ArrayList<>();
//        final Map<String, OffsetToken> tokens = Immutable.transformValues(pages, (key, page) -> OffsetToken.fromToken(page.getPaging()));
//        final Map<String, Integer> offsets =
//        pages.forEach((key, page) -> {
//
//        });

        return Page.empty();
    }

    public enum Stat {

        TOTAL,
        APPROX_TOTAL
    }

    @Data
    @With
    public static class Stats {

        public static final Stats ZERO = new Stats(0L, 0L);

        public static final Stats NULL = new Stats(null, null);

        private final Long approxTotal;

        private final Long total;

        public static Stats fromApproxTotal(final long approxTotal) {

            return new Stats(approxTotal, null);
        }

        public static Stats fromTotal(final long total) {

            return new Stats(total, total);
        }

        // Sum is deliberately poisoned by null
        public static Stats sum(final Stats a, final Stats b) {

            if(a == null || b == null) {
                return NULL;
            } else {
                final Long total = (a.total == null || b.total == null) ? null : a.total + b.total;
                final Long approxTotal = total != null ? total : ((a.approxTotal == null || b.approxTotal == null) ? null : a.approxTotal + b.approxTotal);
                return new Stats(approxTotal, total);
            }
        }
    }

    private final List<T> page;

    private final Token paging;

    private final Stats stats;

    public Page(final List<T> page, final Token paging) {

        this(page, paging, null);
    }

    public static <T> Page<T> empty() {

        return new Page<>(Collections.emptyList(), null, null);
    }

    public static <T> Page<T> single(final T value) {

        return new Page<>(Collections.singletonList(value), null, null);
    }

    public static <T> Page<T> from(final List<T> page) {

        return new Page<>(page, null, null);
    }

    public boolean hasMore() {

        return getPaging() != null;
    }

    public <U> Page<U> map(final Function<? super T, ? extends U> fn) {

        final Page<T> delegate = this;
        final List<U> results = delegate.getPage().stream().map(fn)
                .collect(Collectors.toList());

        return new Page<>(results, paging, stats);
    }

    public Page<T> filter(final Predicate<? super T> fn) {

        final Page<T> delegate = this;
        final List<T> results = delegate.getPage().stream().filter(fn)
                .collect(Collectors.toList());

        return new Page<>(results, paging, stats);
    }

    @Override
    public T get(final int index) {

        return page.get(index);
    }

    @Override
    public int size() {

        return page.size();
    }

    @Data
    public static class Token implements Serializable {

        private static final BaseEncoding ENCODING = BaseEncoding.base64Url().omitPadding();

        private final byte[] value;

        public static final int MAX_SIZE = 10000;

        public Token(final byte[] value) {

            if(value.length  == 0) {
                throw new IllegalStateException("Cannot create empty token");
            } else if(value.length > MAX_SIZE) {
                throw new IllegalStateException("Token is too long (was " + value.length + " bytes)");
            }
            this.value = Arrays.copyOf(value, value.length);
        }

        public Token(final String value) {

            this.value = ENCODING.decode(value);
        }

        @Override
        public String toString() {

            return ENCODING.encode(value);
        }

        public static Token fromStringValue(final String value) {

            return new Token(value.getBytes(Charsets.UTF_8));
        }

        public String getStringValue() {

            return new String(value, Charsets.UTF_8);
        }

        public static Token fromLongValue(final Long value) {

            final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            buffer.putLong(value);
            return new Token(buffer.array());
        }

        public Long getLongValue() {

            final ByteBuffer buffer = ByteBuffer.wrap(getValue());
            return buffer.getLong();
        }

//        public static Token join(final Map<String, Token> tokens) {
//
//            if(tokens.size() == 0) {
//                return null;
//            } else {
//                try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
//                     final DataOutputStream dos = new DataOutputStream(baos)) {
//
//                    for (final Map.Entry<String, Token> entry : Immutable.sortedCopy(tokens).entrySet()) {
//                        final String key = entry.getKey();
//                        final Token token = entry.getValue();
//                        final byte[] keyBytes = key.getBytes(Charsets.UTF_8);
//                        final byte[] valueBytes = token.getValue();
//                        final int keyLength = keyBytes.length;
//                        if(keyLength > MAX_SIZE) {
//                            throw new IllegalStateException("Join token key too long");
//                        }
//                        dos.writeInt(keyLength);
//                        dos.write(keyBytes);
//                        dos.writeInt(valueBytes.length);
//                        dos.write(valueBytes);
//                    }
//                    dos.writeInt(0);
//                    return new Page.Token(baos.toByteArray());
//
//                } catch (final IOException e) {
//                    throw new IllegalStateException(e);
//                }
//            }
//        }
//
//        public static Map<String, Token> split(final Token token) {
//
//            if(token == null) {
//                return Collections.emptyMap();
//            } else {
//                final Map<String, Token> tokens = new HashMap<>();
//                try (final ByteArrayInputStream bais = new ByteArrayInputStream(token.getValue());
//                     final DataInputStream dis = new DataInputStream(bais)) {
//
//                        final int keyLength = dis.readInt();
//                        if(keyLength == 0) {
//                            break;
//                        } else if(keyLength > MAX_SIZE) {
//                            throw new UnsupportedOperationException("Input token key is too long");
//                        } else {
//                            final byte[] keyBytes = new byte[keyLength];
//                            dis.readFully(keyBytes);
//                            final String key = new String(keyBytes, Charsets.UTF_8);
//                            final int valueLength = dis.readInt();
//                            if(valueLength > MAX_SIZE) {
//                                throw new UnsupportedOperationException("Input token value is too long");
//                            }
//                            final byte[] value = new byte[valueLength];
//                            dis.readFully(value);
//                            tokens.put(key, new Token(value));
//                        }
//                    }
//
//                } catch (final IOException e) {
//                    throw new IllegalStateException(e);
//                }
//                return tokens;
//            }
//        }
    }

//    @Data
//    public static class OffsetToken {
//
//        @Nullable
//        private final Token token;
//
//        private final int offset;
//
//        public boolean isComplete() {
//
//            return offset < 0;
//        }
//
//        public static OffsetToken completed() {
//
//            return new OffsetToken(null, -1);
//        }
//
//        public Token toToken() {
//
//            if (offset == 0 && token == null) {
//                return null;
//            }
//
//            try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
//                 final DataOutputStream dos = new DataOutputStream(baos)) {
//
//                dos.writeInt(offset);
//                if (token == null) {
//                    dos.writeInt(0);
//                } else {
//                    dos.writeInt(token.value.length);
//                    dos.write(token.value);
//                }
//                return new Page.Token(baos.toByteArray());
//
//            } catch (final IOException e) {
//                throw new IllegalStateException(e);
//            }
//        }
//
//        public static OffsetToken fromToken(final Token token) {
//
//            if (token == null) {
//                return new OffsetToken(null, 0);
//            }
//
//            try (final ByteArrayInputStream bais = new ByteArrayInputStream(token.getValue());
//                 final DataInputStream dis = new DataInputStream(bais)) {
//
//                final int offset = dis.readInt();
//                final int length = dis.readInt();
//                if (length == 0) {
//                    return new OffsetToken(null, offset);
//                } else if (length > Token.MAX_SIZE) {
//                    throw new UnsupportedOperationException("Input token is too long");
//                } else {
//                    final byte[] value = new byte[length];
//                    dis.readFully(value);
//                    return new OffsetToken(new Token(value), offset);
//                }
//
//            } catch (final IOException e) {
//                throw new IllegalStateException(e);
//            }
//        }
//
//        public static Token join(final Map<String, OffsetToken> tokens) {
//
//            return Token.join(Immutable.transformValues(tokens, (k, v) -> v.toToken()));
//        }
//
//        public static Map<String, OffsetToken> split(final Token token) {
//
//            return Immutable.transformValues(Token.split(token), (k, v) -> fromToken(v));
//        }
//    }
}
