package io.basestar.storage.query;

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

import lombok.Data;

import java.util.Comparator;

public interface Range<T> {

    @Data
    class Bound<T> {

        private final T value;

        private final boolean inclusive;

        static <T> Bound<T> exclusive(final T value) {

            return new Bound<>(value, false);
        }

        static <T> Bound<T> inclusive(final T value) {

            return new Bound<>(value, true);
        }
    }

    default Bound<T> lo() {

        return null;
    }

    default Bound<T> hi() {

        return null;
    }

    default boolean isValid() {

        return true;
    }

    <V> V visit(Visitor<T, V> visitor);

    @SuppressWarnings("unchecked")
    static <T> Range<T> invalid() {

        return (Range<T>) Invalid.INSTANCE;
    }

    static <T> Range<T> eq(final T v) {

        return new Eq<>(v);
    }

    static <T> Range<T> lt(final T lo) {

        return new Lt<>(lo);
    }

    static <T> Range<T> lte(final T lo) {

        return new Lte<>(lo);
    }

    static <T> Range<T> gt(final T hi) {

        return new Gt<>(hi);
    }

    static <T> Range<T> gte(final T hi) {

        return new Gte<>(hi);
    }

    static <T> Range<T> gtLt(final T lo, final T hi) {

        return new GtLt<>(lo, hi);
    }

    static <T> Range<T> gtLte(final T lo, final T hi) {

        return new GtLte<>(lo, hi);
    }

    static <T> Range<T> gteLt(final T lo, final T hi) {

        return new GteLt<>(lo, hi);
    }

    static <T> Range<T> gteLte(final T lo, final T hi) {

        return new GteLte<>(lo, hi);
    }

    static <T> Range<T> and(final Range<T> a, final Range<T> b, final Comparator<T> comparator) {

        if (a.isValid() && b.isValid()) {
            final Bound<T> lo;
            final Bound<T> aLo = a.lo();
            final Bound<T> bLo = b.lo();
            if (aLo == null) {
                lo = bLo;
            } else if (bLo == null) {
                lo = aLo;
            } else {
                final T aValue = aLo.getValue();
                final T bValue = bLo.getValue();
                final boolean aInc = aLo.isInclusive();
                final boolean bInc = bLo.isInclusive();
                final int cmp = comparator.compare(aValue, bValue);
                if (cmp > 0) {
                    lo = new Bound<>(aValue, aInc);
                } else if (cmp < 0) {
                    lo = new Bound<>(bValue, bInc);
                } else {
                    lo = new Bound<>(aValue, aInc && bInc);
                }
            }
            final Bound<T> hi;
            final Bound<T> aHi = a.hi();
            final Bound<T> bHi = b.hi();
            if (aHi == null) {
                hi = bHi;
            } else if (bHi == null) {
                hi = aHi;
            } else {
                final T aValue = aHi.getValue();
                final T bValue = bHi.getValue();
                final boolean aInc = aHi.isInclusive();
                final boolean bInc = bHi.isInclusive();
                final int cmp = comparator.compare(aValue, bValue);
                if (cmp > 0) {
                    hi = new Bound<>(aValue, aInc);
                } else if (cmp < 0) {
                    hi = new Bound<>(bValue, bInc);
                } else {
                    hi = new Bound<>(aValue, aInc && bInc);
                }
            }
            if (lo != null && hi != null) {
                final T loValue = lo.getValue();
                final T hiValue = hi.getValue();
                final boolean loInc = lo.isInclusive();
                final boolean hiInc = hi.isInclusive();
                final int cmp = comparator.compare(loValue, hiValue);
                if (cmp == 0) {
                    if (loInc && hiInc) {
                        return eq(loValue);
                    } else {
                        return invalid();
                    }
                } else if (cmp < 0) {
                    if (loInc && hiInc) {
                        return gteLte(loValue, hiValue);
                    } else if (loInc) {
                        return gteLt(loValue, hiValue);
                    } else if (hiInc) {
                        return gtLte(loValue, hiValue);
                    } else {
                        return gtLt(loValue, hiValue);
                    }
                } else {
                    // lo > hi
                    return invalid();
                }
            } else if (lo != null) {
                final T value = lo.getValue();
                return lo.isInclusive() ? gte(value) : gt(value);
            } else if (hi != null) {
                final T value = hi.getValue();
                return hi.isInclusive() ? lte(value) : lt(value);
            } else {
                return invalid();
            }
        } else {
            return invalid();
        }
    }

    @Data
    class Invalid<T> implements Range<T> {

        private static final Invalid<Object> INSTANCE = new Invalid<>();

        @Override
        public <V> V visit(final Visitor<T, V> visitor) {

            return visitor.visitInvalid();
        }
    }

    @Data
    class Eq<T> implements Range<T> {

        private final T eq;

        @Override
        public Bound<T> lo() {

            return Bound.inclusive(eq);
        }

        @Override
        public Bound<T> hi() {

            return Bound.inclusive(eq);
        }

        @Override
        public <V> V visit(final Visitor<T, V> visitor) {

            return visitor.visitEq(eq);
        }
    }

    @Data
    class Lt<T> implements Range<T> {

        private final T lt;

        @Override
        public Bound<T> hi() {

            return Bound.exclusive(lt);
        }

        @Override
        public <V> V visit(final Visitor<T, V> visitor) {

            return visitor.visitLt(lt);
        }
    }

    @Data
    class Lte<T> implements Range<T> {

        private final T lte;

        @Override
        public Bound<T> hi() {

            return Bound.inclusive(lte);
        }

        @Override
        public <V> V visit(final Visitor<T, V> visitor) {

            return visitor.visitLte(lte);
        }
    }

    @Data
    class Gt<T> implements Range<T> {

        private final T gt;

        @Override
        public Bound<T> lo() {

            return Bound.exclusive(gt);
        }

        @Override
        public <V> V visit(final Visitor<T, V> visitor) {

            return visitor.visitGt(gt);
        }
    }

    @Data
    class Gte<T> implements Range<T> {

        private final T gte;

        @Override
        public Bound<T> lo() {

            return Bound.inclusive(gte);
        }

        @Override
        public <V> V visit(final Visitor<T, V> visitor) {

            return visitor.visitGte(gte);
        }
    }

    @Data
    class GtLt<T> implements Range<T> {

        private final T gt;

        private final T lt;

        @Override
        public Bound<T> lo() {

            return Bound.exclusive(gt);
        }

        @Override
        public Bound<T> hi() {

            return Bound.exclusive(lt);
        }

        @Override
        public <V> V visit(final Visitor<T, V> visitor) {

            return visitor.visitGtLt(gt, lt);
        }
    }

    @Data
    class GtLte<T> implements Range<T> {

        private final T gt;

        private final T lte;

        @Override
        public Bound<T> lo() {

            return Bound.exclusive(gt);
        }

        @Override
        public Bound<T> hi() {

            return Bound.inclusive(lte);
        }

        @Override
        public <V> V visit(final Visitor<T, V> visitor) {

            return visitor.visitGtLte(gt, lte);
        }
    }

    @Data
    class GteLt<T> implements Range<T> {

        private final T gte;

        private final T lt;

        @Override
        public Bound<T> lo() {

            return Bound.inclusive(gte);
        }

        @Override
        public Bound<T> hi() {

            return Bound.exclusive(lt);
        }

        @Override
        public <V> V visit(final Visitor<T, V> visitor) {

            return visitor.visitGteLt(gte, lt);
        }
    }

    @Data
    class GteLte<T> implements Range<T> {

        private final T gte;

        private final T lte;

        @Override
        public Bound<T> lo() {

            return Bound.inclusive(lte);
        }

        @Override
        public Bound<T> hi() {

            return Bound.inclusive(lte);
        }

        @Override
        public <V> V visit(final Visitor<T, V> visitor) {

            return visitor.visitGteLte(gte, lte);
        }
    }

    interface Visitor<T, V> {

        V visitInvalid();

        V visitEq(T eq);

        V visitLt(T lt);

        V visitLte(T lte);

        V visitGt(T gt);

        V visitGte(T gte);

        V visitGtLt(T gt, T lt);

        V visitGtLte(T gt, T lte);

        V visitGteLt(T gte, T lt);

        V visitGteLte(T gte, T lte);
    }
}
