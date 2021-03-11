package io.basestar.database.action;

/*-
 * #%L
 * basestar-database-server
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

import io.basestar.database.event.ObjectDeletedEvent;
import io.basestar.database.options.DeleteOptions;
import io.basestar.event.Event;
import io.basestar.expression.Context;
import io.basestar.schema.Consistency;
import io.basestar.schema.Instance;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.Permission;
import io.basestar.schema.util.ValueContext;
import io.basestar.storage.exception.ObjectMissingException;
import io.basestar.storage.exception.VersionMismatchException;
import io.basestar.util.ISO8601;
import io.basestar.util.Name;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Slf4j
@RequiredArgsConstructor
public class DeleteAction implements Action {

    private static final boolean TOMBSTONE = false;

    private final ObjectSchema schema;

    private final DeleteOptions options;

    @Override
    public ObjectSchema schema() {

        return schema;
    }

    @Override
    public Permission permission(final Instance before) {

        return schema.getPermission(Permission.DELETE);
    }

    @Override
    public String id() {

        return options.getId();
    }

    @Override
    public Instance after(final ValueContext valueContext, final Context expressionContext, final Instance before) {

        final String id = options.getId();

        if (before == null) {
            throw new ObjectMissingException(options.getSchema(), id);
        }

        if(!id.equals(Instance.getId(before))) {
            log.warn("Allowing delete of object {} with mismatched id", id);
        }

        final Name schemaName = Instance.getSchema(before);
        assert schemaName != null;
        if(!schemaName.equals(schema.getQualifiedName())) {
            if(schema.isOrExtending(schemaName)) {
                throw new IllegalStateException("Must delete using actual schema");
            } else {
                log.warn("Allowing delete of object {} with mismatched schema", id);
            }
        }

        final Long beforeVersion = Instance.getVersion(before);
        assert beforeVersion != null;

        if(options.getVersion() != null && !beforeVersion.equals(options.getVersion())) {
            throw new VersionMismatchException(options.getSchema(), id, options.getVersion());
        }

        if(TOMBSTONE) {

            final long afterVersion = beforeVersion + 1;

            final Map<String, Object> tombstone = new HashMap<>();
            final Instant now = ISO8601.now();
            Instance.setId(tombstone, id);
            Instance.setVersion(tombstone, afterVersion);
            Instance.setCreated(tombstone, Instance.getCreated(before));
            Instance.setUpdated(tombstone, now);
            Instance.setHash(tombstone, schema.hash(tombstone));

            return schema.create(tombstone);
        } else {
            return null;
        }
    }

    @Override
    public Set<Name> afterExpand() {

        return Collections.emptySet();
    }

    @Override
    public Event event(final Instance before, final Instance after) {

        final Name schema = Instance.getSchema(before);
        final String id = Instance.getId(before);
        final Long version = Instance.getVersion(before);
        assert version != null;
        return ObjectDeletedEvent.of(schema, id, version, before);
    }

    @Override
    public Set<Name> paths() {

        return Collections.emptySet();
    }

    @Override
    public Consistency getConsistency() {

        return options.getConsistency();
    }
}
