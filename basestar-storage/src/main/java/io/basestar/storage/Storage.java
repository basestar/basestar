package io.basestar.storage;

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

import io.basestar.event.Event;
import io.basestar.expression.Expression;
import io.basestar.schema.*;
import io.basestar.util.Name;
import io.basestar.util.Pager;
import io.basestar.util.Sort;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface Storage {

    interface Builder {

        Storage build();
    }

    enum EventStrategy {

        SUPPRESS,
        EMIT
    }

    ReadTransaction read(Consistency consistency);

    WriteTransaction write(Consistency consistency, Versioning versioning);

    EventStrategy eventStrategy(ReferableSchema schema);

    StorageTraits storageTraits(ReferableSchema schema);

    Set<Name> supportedExpand(LinkableSchema schema, Set<Name> expand);

    default String name() {

        return getClass().getSimpleName();
    }

    void validate(ObjectSchema schema);

    default CompletableFuture<Map<String, Object>> get(final Consistency consistency, final ReferableSchema schema, final String id, final Set<Name> expand) {

        return read(consistency)
                .get(schema, id, expand)
                .read().thenApply(results -> results.get(schema.getQualifiedName(), id));
    }

    default CompletableFuture<Map<String, Object>> getVersion(final Consistency consistency, final ReferableSchema schema, final String id, final long version, final Set<Name> expand) {

        return read(consistency)
                .getVersion(schema, id, version, expand)
                .read().thenApply(results -> results.getVersion(schema.getQualifiedName(), id, version));
    }

    Pager<Map<String, Object>> query(LinkableSchema schema, Expression query, List<Sort> sort, Set<Name> expand);

    CompletableFuture<Set<Event>> afterCreate(ObjectSchema schema, String id, Map<String, Object> after);

    CompletableFuture<Set<Event>> afterUpdate(ObjectSchema schema, String id, long version, Map<String, Object> before, Map<String, Object> after);

    CompletableFuture<Set<Event>> afterDelete(ObjectSchema schema, String id, long version, Map<String, Object> before);

    default Scan scan(final ReferableSchema schema, final Expression query, final int segments) {

        throw new UnsupportedOperationException();
    }

    interface ReadTransaction {

        ReadTransaction get(ReferableSchema schema, String id, Set<Name> expand);

        ReadTransaction getVersion(ReferableSchema schema, String id, long version, Set<Name> expand);

        CompletableFuture<BatchResponse> read();

        interface Delegating extends ReadTransaction {

            ReadTransaction delegate(ReferableSchema schema);

            @Override
            default ReadTransaction get(final ReferableSchema schema, final String id, final Set<Name> expand) {

                delegate(schema).get(schema, id, expand);
                return this;
            }

            @Override
            default ReadTransaction getVersion(final ReferableSchema schema, final String id, final long version, final Set<Name> expand) {

                delegate(schema).getVersion(schema, id, version, expand);
                return this;
            }
        }
    }

    interface WriteTransaction {

        WriteTransaction writeView(ViewSchema schema, Map<String, Object> before, Map<String, Object> after);

        WriteTransaction createObject(ObjectSchema schema, String id, Map<String, Object> after);

        WriteTransaction updateObject(ObjectSchema schema, String id, Map<String, Object> before, Map<String, Object> after);

        WriteTransaction deleteObject(ObjectSchema schema, String id, Map<String, Object> before);

        WriteTransaction writeHistory(ObjectSchema schema, String id, Map<String, Object> after);

        CompletableFuture<BatchResponse> write();

        interface Delegating extends WriteTransaction {

            WriteTransaction delegate(final LinkableSchema schema);

            @Override
            default WriteTransaction writeView(final ViewSchema schema, final Map<String, Object> before, final Map<String, Object> after) {

                delegate(schema).writeView(schema, before, after);
                return this;
            }

            @Override
            default WriteTransaction createObject(final ObjectSchema schema, final String id, final Map<String, Object> after) {

                delegate(schema).createObject(schema, id, after);
                return this;
            }

            @Override
            default WriteTransaction updateObject(final ObjectSchema schema, final String id, final Map<String, Object> before, final Map<String, Object> after) {

                delegate(schema).updateObject(schema, id, before, after);
                return this;
            }

            @Override
            default WriteTransaction deleteObject(final ObjectSchema schema, final String id, final Map<String, Object> before) {

                delegate(schema).deleteObject(schema, id, before);
                return this;
            }

            @Override
            default WriteTransaction writeHistory(final ObjectSchema schema, final String id, final Map<String, Object> after) {

                delegate(schema).writeHistory(schema, id, after);
                return this;
            }
        }
    }
}