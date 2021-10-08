package io.basestar.util;

/*-
 * #%L
 * basestar-spark
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

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.io.Serializable;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings("all")
public enum BucketFunction implements Serializable {

    MD5(Hashing.md5()),
    MURMER3_32(Hashing.murmur3_32()),
    ADLER_32(Hashing.adler32());

    private final HashFunction hash;

    BucketFunction(final HashFunction hash) {

        this.hash = hash;
    }

    public int apply(final int count, final Object... input) {

        return apply(count, Stream.of(input).map(Objects::toString).collect(Collectors.joining()));
    }

    public int apply(final int count, final String input) {

        if (count < 2) {
            throw new IllegalStateException("Count must be at least 2");
        }
        final BigInteger value = new BigInteger(hash.hashString(input, StandardCharsets.UTF_8).asBytes());
        return value.mod(BigInteger.valueOf(count)).intValue();
    }
}
