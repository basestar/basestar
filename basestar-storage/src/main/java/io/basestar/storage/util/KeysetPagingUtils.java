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
