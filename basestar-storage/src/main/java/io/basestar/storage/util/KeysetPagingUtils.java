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
import io.basestar.schema.use.UseBinary;
import io.basestar.schema.use.UseInteger;
import io.basestar.storage.exception.PagingTokenSyntaxException;
import io.basestar.util.Bytes;
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
        if (sort == null) {
            return ImmutableList.of(Sort.asc(id));
        } else {
            final List<Sort> result = new ArrayList<>();
            for (final Sort s : sort) {
                result.add(s);
                if (s.getName().equals(id)) {
                    // Other sort paths are irrelevant
                    return result;
                }
            }
            result.add(Sort.asc(id));
            return result;
        }
    }

    public static List<Object> keysetValues(final InstanceSchema schema,
                                            final List<Sort> sortingInfo,
                                            final Page.Token token) {

        final byte[] bytes = token.getValue();
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
             final DataInputStream dis = new DataInputStream(bais)) {
            final List<Object> sortValues = new ArrayList<>();
            for (final Sort sort : sortingInfo) {
                final Use<Object> type = schema.typeOf(sort.getName());
                final Object value = type.deserialize(dis);
                sortValues.add(value);
            }
            assert sortValues.size() == sortingInfo.size();
            return sortValues;
        } catch (final IOException e) {
            // Shouldn't be possible, not doing real IO
            throw new PagingTokenSyntaxException(e.getMessage());
        }
    }

    public static Page.Token keysetPagingToken(final InstanceSchema schema,
                                               final List<Sort> shortingInfo,
                                               final Map<String, Object> object) {

        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final DataOutputStream dos = new DataOutputStream(baos)) {
            for (final Sort sort : shortingInfo) {
                final Name name = sort.getName();
                final Use<Object> type = schema.typeOf(name);
                final Object value = type.create(name.get(object));
                type.serialize(value, dos);
            }
            dos.flush();
            return new Page.Token(baos.toByteArray());
        } catch (final IOException e) {
            // Shouldn't be possible, not doing real IO
            throw new PagingTokenSyntaxException(e.getMessage());
        }
    }

    public static Page.Token countPreservingToken(final Page.Token token,
                                                  final long total) {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final DataOutputStream dos = new DataOutputStream(baos)) {
            UseInteger.DEFAULT.serialize(total, dos);
            UseBinary.DEFAULT.serialize(new Bytes(token.getValue()), dos);
            dos.flush();
            return new Page.Token(baos.toByteArray());
        } catch (final IOException e) {
            throw new PagingTokenSyntaxException(e.getMessage());
        }
    }

    public static CountPreservingTokenInfo countPreservingTokenInfo(final Page.Token token) {
        final byte[] bytes = token.getValue();
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
             final DataInputStream dis = new DataInputStream(bais)) {
            final long total = UseInteger.DEFAULT.deserialize(dis);
            final Bytes nestedTokenBytes = UseBinary.DEFAULT.deserialize(dis);
            final Page.Token nestedToken = new  Page.Token(nestedTokenBytes.getBytes());
            return new CountPreservingTokenInfo(total, nestedToken);
        } catch (final IOException e) {
            throw new PagingTokenSyntaxException(e.getMessage());
        }
    }
}
