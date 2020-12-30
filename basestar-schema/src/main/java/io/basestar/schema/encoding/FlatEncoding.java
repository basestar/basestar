package io.basestar.schema.encoding;

import io.basestar.encoding.Encoding;
import io.basestar.util.Immutable;

import java.util.*;

public class FlatEncoding implements Encoding<Map<String, Object>, Map<String, Object>> {

    @Override
    public Map<String, Object> encode(final Map<String, Object> input) {

        final Map<String, Object> result = new HashMap<>();
        input.forEach((key, value) -> encode(result, key, value));
        return Immutable.map(result);
    }

    private static void encode(final Map<String, Object> output, final String key, final Object value) {

        if (value instanceof Map<?, ?>) {
            for (final Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                encode(output, key + "." + entry.getKey(), entry.getValue());
            }
        } else if (value instanceof Collection<?>) {
            int offset = 0;
            for (final Object v : (Collection<?>) value) {
                encode(output, key + "[" + offset + "]", v);
                ++offset;
            }
        } else {
            output.put(key, value);
        }
    }

    @Override
    public Map<String, Object> decode(final Map<String, Object> input) {

        final Map<String, Object> result = new HashMap<>();
        input.forEach((key, value) -> decode(result, key, value));
        return Immutable.map(result);
    }

    @SuppressWarnings("unchecked")
    private static void decode(final Map<String, Object> output, final String key, final Object value) {

        final int firstBracket = key.indexOf("[");
        final int firstDot = key.indexOf(".");
        if(firstBracket < 0 && firstDot < 0) {
            assert !output.containsKey(key);
            output.put(key, value);
        } else if (firstBracket >= 0 && (firstDot < 0 || firstBracket < firstDot)) {
            final String keyPrefix = key.substring(0, firstBracket);
            final int nextBracket = key.indexOf("]", firstBracket + 1);
            if(nextBracket < 0) {
                throw new IllegalStateException("Key " + key + " is malformed");
            } else {
                final int index = Integer.parseInt(key.substring(firstBracket + 1, nextBracket));
                final List<Object> target;
                if(output.containsKey(keyPrefix)) {
                    final Object tmp = output.get(keyPrefix);
                    if (tmp instanceof List) {
                        target = (List<Object>) output.get(keyPrefix);
                    } else {
                        throw new IllegalStateException("Input is malformed at " + key + " (not a list)");
                    }
                } else {
                    target = new ArrayList<>();
                    output.put(keyPrefix, target);
                }
                while(target.size() <= index) {
                    target.add(null);
                }
                if(nextBracket + 1 == key.length()) {
                    target.set(index, value);
                } else {
                    final String rest = key.substring(nextBracket + 2);
                    final Map<String, Object> newTarget;
                    final Object tmp = target.get(index);
                    if(tmp == null) {
                        newTarget = new HashMap<>();
                        target.set(index, newTarget);
                    } else if (tmp instanceof Map) {
                        newTarget = (Map<String, Object>)tmp;
                    } else {
                        throw new IllegalStateException("Input is malformed at " + key + " (not a list)");
                    }
                    decode(newTarget, rest, value);
                }
            }
        } else {
            final String keyPrefix = key.substring(0, firstDot);
            final Map<String, Object> target;
            if(output.containsKey(keyPrefix)) {
                final Object tmp = output.get(keyPrefix);
                if(tmp instanceof Map) {
                    target = (Map<String, Object>) output.get(keyPrefix);
                } else {
                    throw new IllegalStateException("Input is malformed at " + key + " (not a map)");
                }
            } else {
                target = new HashMap<>();
                output.put(keyPrefix, target);
            }
            final String rest = key.substring(firstDot + 1);
            decode(target, rest, value);
        }
    }
}
