package io.basestar.expression.methods;

import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.basestar.expression.type.Coercion;
import io.basestar.util.Bytes;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@SuppressWarnings("unused")
public class ObjectMethods implements Serializable {

    public Boolean toboolean(final Object value) {

        return Coercion.toBoolean(value);
    }

    public Long tointeger(final Object value) {

        return Coercion.toInteger(value);
    }

    public Double tonumber(final Object value) {

        return Coercion.toFloat(value);
    }

    public String tostring(final Object value) {

        return Coercion.toString(value);
    }

    public Bytes tobinary(final Object value) {

        return Coercion.toBinary(value);
    }

    public LocalDate todate(final Object value) {

        return Coercion.toDate(value);
    }

    public Instant todatetime(final Object value) {

        return Coercion.toDateTime(value);
    }

    public Bytes murmur(final Object value) {

        return murmur(value, null);
    }

    @SuppressWarnings({"UnstableApiUsage"})
    public Bytes murmur(final Object value, final Long bytes) {

        final HashFunction hashing;
        final int length;
        if (bytes != null) {
            if (bytes > 16) {
                throw new IllegalStateException("Byte length " + bytes + " for murmur not valid (must be <= 16)");
            } else if (bytes < 1) {
                throw new IllegalStateException("Byte length " + bytes + " for murmur not valid (must be >= 1)");
            } else if (bytes <= 4) {
                hashing = Hashing.murmur3_32_fixed();
                length = bytes.intValue();
            } else {
                hashing = Hashing.murmur3_128();
                length = bytes.intValue();
            }
        } else {
            hashing = Hashing.murmur3_128();
            length = 16;
        }
        if (value == null) {
            return null;
        } else {
            final String digest = digest(value);
            final HashCode hash = hashing.hashString(digest, Charsets.UTF_8);
            return Bytes.valueOf(hash.asBytes()).xorFold(length);
        }
    }

    private static String digest(final Object value) {

        if (value == null) {
            return "";
        } else if (value instanceof Map) {
            return "{" + ((Map<?, ?>) value).entrySet().stream().sorted()
                    .map(ObjectMethods::digest)
                    .collect(Collectors.joining(",")) + "}";
        } else if (value instanceof Set) {
            return "[" + ((Set<?>) value).stream().sorted()
                    .map(ObjectMethods::digest)
                    .collect(Collectors.joining(",")) + "]";
        } else if (value instanceof Collection) {
            return "[" + ((Collection<?>) value).stream()
                    .map(ObjectMethods::digest)
                    .collect(Collectors.joining(",")) + "]";
        } else {
            return value.toString();
        }
    }
}
