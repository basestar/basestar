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
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.use.Use;
import io.basestar.util.Name;
import io.basestar.util.Page;
import io.basestar.util.Sort;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KeysetPagingUtils {

    private KeysetPagingUtils() {

    }

    public static List<Sort> normalizeSort(final LinkableSchema schema, final List<Sort> sort) {

        final Name id = Name.of(schema.id());
        if(sort == null) {
            return ImmutableList.of(Sort.asc(id));
        } else {
            final List<Sort> result = new ArrayList<>();
            for(final Sort s : sort) {
                result.add(s);
                if(s.getName().equals(id)) {
                    // Other sort paths are irrelevant
                    return result;
                }
            }
            result.add(Sort.asc(id));
            return result;
        }
    }

    public static List<Object> keysetValues(final InstanceSchema schema, final List<Sort> sort, final Page.Token token) {

        final byte[] bytes = token.getValue();
        final List<Object> values = new ArrayList<>();
        try(final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            final DataInputStream dis = new DataInputStream(bais)) {
            for(final Sort s : sort) {
                final Name name = s.getName();
                final Use<Object> type = schema.typeOf(name);
                final Object value = type.deserialize(dis);
                values.add(value);
            }
        } catch (final IOException e) {
            // Shouldn't be possible, not doing real IO
            throw new UncheckedIOException(e);
        }
        assert values.size() == sort.size();
        return values;
    }

    public static Page.Token keysetPagingToken(final InstanceSchema schema, final List<Sort> sort, final Map<String, Object> object) {

        try(final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final DataOutputStream dos = new DataOutputStream(baos)) {
            for (final Sort s : sort) {
                final Name name = s.getName();
                final Use<Object> type = schema.typeOf(name);
                final Object value = type.create(name.get(object));
                type.serialize(value, dos);
            }
            dos.flush();
            return new Page.Token(baos.toByteArray());
        } catch (final IOException e) {
            // Shouldn't be possible, not doing real IO
            throw new UncheckedIOException(e);
        }
    }
}
