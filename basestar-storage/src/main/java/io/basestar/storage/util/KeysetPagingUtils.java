package io.basestar.storage.util;

import com.google.common.collect.ImmutableList;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.Reserved;
import io.basestar.schema.use.Use;
import io.basestar.util.PagingToken;
import io.basestar.util.Path;
import io.basestar.util.Sort;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KeysetPagingUtils {

    public static List<Sort> normalizeSort(final List<Sort> sort) {

        if(sort == null) {
            return ImmutableList.of(Sort.asc(Path.of(Reserved.ID)));
        } else {
            final List<Sort> result = new ArrayList<>();
            for(final Sort s : sort) {
                result.add(s);
                if(s.getPath().equalsSingle(Reserved.ID)) {
                    // Other sort paths are irrelevant
                    return result;
                }
            }
            result.add(Sort.asc(Path.of(Reserved.ID)));
            return result;
        }
    }

    public static List<Object> keysetValues(final ObjectSchema schema, final List<Sort> sort, final PagingToken token) {

        final byte[] bytes = token.getValue();
        final List<Object> values = new ArrayList<>();
        try(final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            final DataInputStream dis = new DataInputStream(bais)) {
            for(final Sort s : sort) {
                final Path path = s.getPath();
                final Use<Object> type = schema.typeOf(path);
                final Object value = type.deseralize(dis);
                values.add(value);
            }
        } catch (final IOException e) {
            // Shouldn't be possible, not doing real IO
            throw new IllegalStateException(e);
        }
        assert values.size() == sort.size();
        return values;
    }

    public static PagingToken keysetPagingToken(final ObjectSchema schema, final List<Sort> sort, final Map<String, Object> object) {

        try(final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final DataOutputStream dos = new DataOutputStream(baos)) {
            for (final Sort s : sort) {
                final Path path = s.getPath();
                final Object value = path.apply(object);
                final Use<Object> type = schema.typeOf(path);
                type.serialize(value, dos);
            }
            dos.flush();
            return new PagingToken(baos.toByteArray());
        } catch (final IOException e) {
            // Shouldn't be possible, not doing real IO
            throw new IllegalStateException(e);
        }
    }
}
