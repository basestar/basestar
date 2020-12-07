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

//    @Deprecated
//    Pager<RepairInfo> repair(LinkableSchema schema);
//
//    @Deprecated
//    Pager<RepairInfo> repairIndex(LinkableSchema schema, Index index);

    void validate(ObjectSchema schema);

    CompletableFuture<Set<Event>> afterCreate(ObjectSchema schema, String id, Map<String, Object> after);

    CompletableFuture<Set<Event>> afterUpdate(ObjectSchema schema, String id, long version, Map<String, Object> before, Map<String, Object> after);

    CompletableFuture<Set<Event>> afterDelete(ObjectSchema schema, String id, long version, Map<String, Object> before);

    Pager<Map<String, Object>> queryInterface(InterfaceSchema schema, Expression query, List<Sort> sort, Set<Name> expand);

    Pager<Map<String, Object>> queryObject(ObjectSchema schema, Expression query, List<Sort> sort, Set<Name> expand);

    Pager<Map<String, Object>> queryView(ViewSchema schema, Expression query, List<Sort> sort, Set<Name> expand);

    interface ReadTransaction {

        ReadTransaction getInterface(InterfaceSchema schema, String id, Set<Name> expand);

        ReadTransaction getInterfaceVersion(InterfaceSchema schema, String id, long version, Set<Name> expand);

        ReadTransaction getObject(ObjectSchema schema, String id, Set<Name> expand);

        ReadTransaction getObjectVersion(ObjectSchema schema, String id, long version, Set<Name> expand);
//
        CompletableFuture<BatchResponse> read();

        interface Delegating extends ReadTransaction {

            ReadTransaction delegate();

            @Override
            default ReadTransaction getInterface(final InterfaceSchema schema, final String id, final Set<Name> expand) {

                delegate().getInterface(schema, id, expand);
                return this;
            }

            @Override
            default ReadTransaction getInterfaceVersion(final InterfaceSchema schema, final String id, final long version, final Set<Name> expand) {

                delegate().getInterfaceVersion(schema, id, version, expand);
                return this;
            }

            @Override
            default ReadTransaction getObject(final ObjectSchema schema, final String id, final Set<Name> expand) {

                delegate().getObject(schema, id, expand);
                return this;
            }

            @Override
            default ReadTransaction getObjectVersion(final ObjectSchema schema, final String id, final long version, final Set<Name> expand) {

                delegate().getObjectVersion(schema, id, version, expand);
                return this;
            }

            @Override
            default CompletableFuture<BatchResponse> read() {

                return delegate().read();
            }
        }
    }

    interface WriteTransaction {

        WriteTransaction createObject(ObjectSchema schema, String id, Map<String, Object> after);

        WriteTransaction updateObject(ObjectSchema schema, String id, Map<String, Object> before, Map<String, Object> after);

        WriteTransaction deleteObject(ObjectSchema schema, String id, Map<String, Object> before);

        CompletableFuture<BatchResponse> write();

        interface Delegating extends WriteTransaction {

            WriteTransaction delegate();

            @Override
            default WriteTransaction createObject(final ObjectSchema schema, final String id, final Map<String, Object> after) {

                delegate().createObject(schema, id, after);
                return this;
            }

            @Override
            default WriteTransaction updateObject(final ObjectSchema schema, final String id, final Map<String, Object> before, final Map<String, Object> after) {

                delegate().updateObject(schema, id, before, after);
                return this;
            }

            @Override
            default WriteTransaction deleteObject(final ObjectSchema schema, final String id, final Map<String, Object> before) {

                delegate().deleteObject(schema, id, before);
                return this;
            }

//            @Override
//            default WriteTransaction writeHistory(final ObjectSchema schema, final String id, final long version, final Map<String, Object> after) {
//
//                delegate().writeHistory(schema, id, version, after);
//                return this;
//            }
//
//            @Override
//            default WriteTransaction createIndex(final ReferableSchema schema, final Index index, final String id, final long version, final Index.Key key, final Map<String, Object> projection) {
//
//                delegate().createIndex(schema, index, id, version, key, projection);
//                return this;
//            }
//
//            @Override
//            default WriteTransaction updateIndex(final ReferableSchema schema, final Index index, final String id, final long version, final Index.Key key, final Map<String, Object> projection) {
//
//                delegate().createIndex(schema, index, id, version, key, projection);
//                return this;
//            }
//
//            @Override
//            default WriteTransaction deleteIndex(final ReferableSchema schema, final Index index, final String id, final long version, final Index.Key key) {
//
//                delegate().deleteIndex(schema, index, id, version, key);
//                return this;
//            }

            @Override
            default CompletableFuture<BatchResponse> write() {

                return delegate().write();
            }
        }
    }

    default CompletableFuture<Map<String, Object>> get(final Consistency consistency, final ReferableSchema schema, final String id, final Set<Name> expand) {

        if(schema instanceof InterfaceSchema) {
            return getInterface(consistency, (InterfaceSchema)schema, id, expand);
        } else {
            return getObject(consistency, (ObjectSchema)schema, id, expand);
        }
    }

    default CompletableFuture<Map<String, Object>> getVersion(final Consistency consistency, final ReferableSchema schema, final String id, final long version, final Set<Name> expand) {

        if(schema instanceof InterfaceSchema) {
            return getInterfaceVersion(consistency, (InterfaceSchema)schema, id, version, expand);
        } else {
            return getObjectVersion(consistency, (ObjectSchema)schema, id, version, expand);
        }
    }

    default Pager<Map<String, Object>> query(final LinkableSchema schema, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        if(schema instanceof ViewSchema) {
            return queryView((ViewSchema)schema, query, sort, expand);
        } else if(schema instanceof InterfaceSchema) {
            return queryInterface((InterfaceSchema)schema, query, sort, expand);
        } else {
            return queryObject((ObjectSchema)schema, query, sort, expand);
        }
    }

    default CompletableFuture<Map<String, Object>> getObject(final Consistency consistency, final ObjectSchema schema, final String id, final Set<Name> expand) {

        return read(consistency)
                .getObject(schema, id, expand)
                .read().thenApply(results -> results.get(schema.getQualifiedName(), id));
    }

    default CompletableFuture<Map<String, Object>> getObjectVersion(final Consistency consistency, final ObjectSchema schema, final String id, final long version, final Set<Name> expand) {

        return read(consistency)
                .getObjectVersion(schema, id, version, expand)
                .read().thenApply(results -> results.getVersion(schema.getQualifiedName(), id, version));
    }

    default CompletableFuture<Map<String, Object>> getInterface(final Consistency consistency, final InterfaceSchema schema, final String id, final Set<Name> expand) {

        return read(consistency)
                .getInterface(schema, id, expand)
                .read().thenApply(results -> results.get(schema.getQualifiedName(), id));
    }

    default CompletableFuture<Map<String, Object>> getInterfaceVersion(final Consistency consistency, final InterfaceSchema schema, final String id, final long version, final Set<Name> expand) {

        return read(consistency)
                .getInterfaceVersion(schema, id, version, expand)
                .read().thenApply(results -> results.getVersion(schema.getQualifiedName(), id, version));
    }
//
//    default CompletableFuture<?> asyncIndexCreated(final ObjectSchema schema, final Index index, String id, final long version, final Index.Key key, final Map<String, Object> projection) {
//
//        final WriteTransaction write = write(Consistency.ASYNC, Versioning.CHECKED);
//        write.createIndex(schema, index, id, version, key, projection);
//        return write.write();
//    }
//
//    default CompletableFuture<?> asyncIndexUpdated(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key, final Map<String, Object> projection) {
//
//        final WriteTransaction write = write(Consistency.ASYNC, Versioning.CHECKED);
//        write.updateIndex(schema, index, id, version, key, projection);
//        return write.write();
//    }
//
//    default CompletableFuture<?> asyncIndexDeleted(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key) {
//
//        final WriteTransaction write = write(Consistency.ASYNC, Versioning.CHECKED);
//        write.deleteIndex(schema, index, id, version, key);
//        return write.write();
//    }
//
//    default CompletableFuture<?> asyncHistoryCreated(final ObjectSchema schema, String id, final long version, Map<String, Object> after) {
//
//        final WriteTransaction write = write(Consistency.ASYNC, Versioning.CHECKED);
//        write.writeHistory(schema, id, version, after);
//        return write.write();
//    }


//    interface WithoutWriteHistory extends Storage {
//
//        @Override
//        default CompletableFuture<?> asyncHistoryCreated(final ObjectSchema schema, String id, final long version, Map<String, Object> after) {
//
//            throw new UnsupportedOperationException();
//        }
//    }

//    interface WithoutWriteIndex extends Storage {
//
//        @Override
//        default CompletableFuture<?> asyncIndexCreated(final ObjectSchema schema, final Index index, String id, final long version, final Index.Key key, final Map<String, Object> projection) {
//
//            throw new UnsupportedOperationException();
//        }
//
//        @Override
//        default CompletableFuture<?> asyncIndexUpdated(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key, final Map<String, Object> projection) {
//
//            throw new UnsupportedOperationException();
//        }
//
//        @Override
//        default CompletableFuture<?> asyncIndexDeleted(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key) {
//
//            throw new UnsupportedOperationException();
//        }
//    }

//    interface WithWriteHistory extends Storage {
//
//        @Override
//        WriteTransaction write(Consistency consistency, Versioning versioning);
//
//        @Override
//        default CompletableFuture<?> asyncHistoryCreated(final ObjectSchema schema, String id, final long version, Map<String, Object> after) {
//
//            final WriteTransaction write = write(Consistency.ASYNC, Versioning.CHECKED);
//            write.createHistory(schema, id, version, after);
//            return write.write();
//        }
//
//        interface WriteTransaction extends Storage.WriteTransaction {
//
//            WriteTransaction createHistory(ObjectSchema schema, String id, long version, Map<String, Object> after);
//        }
//    }

//    interface WithWriteIndex extends Storage {
//
//        @Override
//        WriteTransaction write(Consistency consistency, Versioning versioning);
//
//        @Override
//        default CompletableFuture<?> asyncIndexCreated(final ObjectSchema schema, final Index index, String id, final long version, final Index.Key key, final Map<String, Object> projection) {
//
//            final WriteTransaction write = write(Consistency.ASYNC, Versioning.CHECKED);
//            write.createIndex(schema, index, id, version, key, projection);
//            return write.write();
//        }
//
//        @Override
//        default CompletableFuture<?> asyncIndexUpdated(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key, final Map<String, Object> projection) {
//
//            final WriteTransaction write = write(Consistency.ASYNC, Versioning.CHECKED);
//            write.updateIndex(schema, index, id, version, key, projection);
//            return write.write();
//        }
//
//        @Override
//        default CompletableFuture<?> asyncIndexDeleted(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key) {
//
//            final WriteTransaction write = write(Consistency.ASYNC, Versioning.CHECKED);
//            write.deleteIndex(schema, index, id, version, key);
//            return write.write();
//        }
//
//        interface WriteTransaction extends Storage.WriteTransaction {
//
//            WriteTransaction createIndex(ReferableSchema schema, Index index, String id, long version, Index.Key key, Map<String, Object> projection);
//
//            WriteTransaction updateIndex(ReferableSchema schema, Index index, String id, long version, Index.Key key, Map<String, Object> projection);
//
//            WriteTransaction deleteIndex(ReferableSchema schema, Index index, String id, long version, Index.Key key);
//        }
//    }
}