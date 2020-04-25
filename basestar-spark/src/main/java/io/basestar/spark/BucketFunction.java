package io.basestar.spark;

import com.google.common.base.Charsets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.io.Serializable;
import java.math.BigInteger;

@SuppressWarnings("UnstableApiUsage")
public interface BucketFunction extends Serializable {

    // Bucket using a configurable prefix of the MD5 of the input, useful when you need a uniform bucketing
    // function built from commonly available DBMS functions

    @SuppressWarnings("deprecation")
    static HashPrefix md5Prefix(final int len) {

        return substring(Hashing.md5(), len);
    }

    // Bucket using modulo of the murmer3 (32 bit) function, may not be able to reproduce in a DBMS

    static HashModulo murmer3Modulo(final int modulo) {

        return modulo(Hashing.murmur3_32(), modulo);
    }

    static HashModulo adler32Modulo(final int modulo) {

        return modulo(Hashing.adler32(), modulo);
    }

    static HashPrefix substring(final HashFunction hash, final int len) {

        return new HashPrefix(hash, len);
    }

    static HashModulo modulo(final HashFunction hash, final int modulo) {

        return new HashModulo(hash, modulo);
    }

    String apply(String input);

    class HashPrefix implements BucketFunction {

        public static final int DEFAULT_LEN = 2;

        private final HashFunction hash;

        private final int len;

        public HashPrefix(final HashFunction hash) {

            this(hash, DEFAULT_LEN);
        }

        public HashPrefix(final HashFunction hash, final int len) {

            this.hash = hash;
            this.len = len;
            if(len < 1) {
                throw new IllegalStateException("Substring length must be at least 1");
            }
            if(len > 4) {
                throw new IllegalStateException("Substring longer than 4 will create over 1M buckets");
            }
        }

        @Override
        public String apply(final String input) {

            final String str = hash.hashString(input, Charsets.UTF_8).toString();
            return str.substring(0, len);
        }
    }

    class HashModulo implements BucketFunction {

        public static final int DEFAULT_RADIX = 16;

        private final HashFunction hash;

        private final int modulo;

        private final int radix;

        public HashModulo(final HashFunction hash, final int modulo) {

            this(hash, modulo, DEFAULT_RADIX);
        }

        public HashModulo(final HashFunction hash, final int modulo, final int radix) {

            this.hash = hash;
            this.modulo = modulo;
            this.radix = radix;
            if(modulo < 2) {
                throw new IllegalStateException("Modulo must be at least 2");
            }
        }

        @Override
        public String apply(final String input) {

            final BigInteger value = new BigInteger(hash.hashString(input, Charsets.UTF_8).asBytes());
            return value.mod(BigInteger.valueOf(modulo)).toString(radix);
        }
    }
}
