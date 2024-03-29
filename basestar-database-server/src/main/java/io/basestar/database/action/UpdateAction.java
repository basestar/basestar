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

import io.basestar.database.CommonVars;
import io.basestar.database.event.ObjectCreatedEvent;
import io.basestar.database.event.ObjectUpdatedEvent;
import io.basestar.database.options.UpdateOptions;
import io.basestar.event.Event;
import io.basestar.expression.Context;
import io.basestar.schema.*;
import io.basestar.schema.exception.ConstraintViolationException;
import io.basestar.schema.util.ValueContext;
import io.basestar.storage.exception.ObjectMissingException;
import io.basestar.storage.exception.VersionMismatchException;
import io.basestar.util.ISO8601;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import lombok.RequiredArgsConstructor;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class UpdateAction implements Action {

    private final ObjectSchema schema;

    private final UpdateOptions options;

    @Override
    public ObjectSchema schema() {

        return schema;
    }

    @Override
    public Permission permission(final Instance before) {

        if (before == null) {
            return schema.getPermission(Permission.CREATE);
        } else {
            return schema.getPermission(Permission.UPDATE);
        }
    }

    @Override
    public String id() {

        return options.getId();
    }

    @Override
    public Instance after(final ValueContext valueContext, final Context expressionContext, final Instance before) {

        final String id = options.getId();

        final Map<String, Object> data = new HashMap<>();
        final UpdateOptions.Mode mode = Nullsafe.orDefault(options.getMode(), UpdateOptions.Mode.REPLACE);

        final long afterVersion;
        final Instant created;
        final Instant updated = ISO8601.now();

        if (before == null) {
            if (mode == UpdateOptions.Mode.CREATE) {
                afterVersion = 1L;
                created = updated;
            } else {
                throw new ObjectMissingException(options.getSchema(), id);
            }
        } else {
            if (!schema.getQualifiedName().equals(Instance.getSchema(before))) {
                throw new IllegalStateException("Cannot change instance schema");
            }

            if (mode == UpdateOptions.Mode.MERGE || mode == UpdateOptions.Mode.MERGE_DEEP) {
                data.putAll(before);
            }

            final Long beforeVersion = Instance.getVersion(before);
            assert beforeVersion != null;

            if (options.getVersion() != null && !beforeVersion.equals(options.getVersion())) {
                throw new VersionMismatchException(options.getSchema(), id, options.getVersion());
            }

            afterVersion = beforeVersion + 1;
            created = Instance.getCreated(before);
        }

        if (options.getData() != null) {
            if (mode == UpdateOptions.Mode.MERGE_DEEP) {
                mergeDeep(data, options.getData());
            } else {
                data.putAll(options.getData());
            }
        }
        if (options.getExpressions() != null) {
            options.getExpressions().forEach((k, expr) -> data.put(k, expr.evaluate(expressionContext)));
        }
        final Map<String, Object> initial = new HashMap<>(schema.create(valueContext, data, null));

        Instance.setId(initial, id);
        Instance.setSchema(initial, schema.getQualifiedName());
        Instance.setCreated(initial, created);
        if (before != null) {
            Instance.setVersion(initial, Instance.getVersion(before));
            Instance.setUpdated(initial, Instance.getUpdated(before));
        }

        if (!schema.areEqual(before, initial)) {
            Instance.setVersion(initial, afterVersion);
            Instance.setUpdated(initial, updated);
        }
        Instance.setHash(initial, schema.hash(initial));

        final Instance evaluated = schema.evaluateProperties(expressionContext.with(CommonVars.VAR_THIS, initial), new Instance(initial), Collections.emptySet());

        final Set<Constraint.Violation> violations = schema.validate(expressionContext.with(CommonVars.VAR_THIS, evaluated), before == null ? evaluated : before, evaluated);
        if (!violations.isEmpty()) {
            throw new ConstraintViolationException(violations);
        }

        return evaluated;
    }

    @SuppressWarnings("unchecked")
    private void mergeDeep(final Map<String, Object> target, final Map<String, Object> source) {

        for (final Map.Entry<String, Object> e : source.entrySet()) {
            final String key = e.getKey();
            final Object merging = e.getValue();
            final Object current = target.get(key);
            if (current instanceof Map && merging instanceof Map) {
                final Map<String, Object> merged = new HashMap<>((Map<String, Object>) current);
                mergeDeep(merged, (Map<String, Object>) merging);
                target.put(key, merged);
            } else {
                target.put(key, merging);
            }
        }
    }

    @Override
    public Set<Name> afterExpand() {

        return options.getExpand();
    }

    @Override
    public Event event(final Instance before, final Instance after) {

        if (before == null) {
            final Name schema = Instance.getSchema(after);
            final String id = Instance.getId(after);
            return ObjectCreatedEvent.of(schema, id, after);
        } else {
            final Name schema = Instance.getSchema(before);
            final String id = Instance.getId(before);
            final Long version = Instance.getVersion(before);
            assert version != null;
            return ObjectUpdatedEvent.of(schema, id, version, before, after);
        }
    }

    @Override
    public Set<Name> paths() {

        // FIXME: shouldn't have to bind here, need to fix multi-part path constants in parser
        return Nullsafe.orDefault(options.getExpressions()).values().stream()
                .flatMap(e -> e.bind(Context.init()).names().stream())
                .collect(Collectors.toSet());
    }

    @Override
    public Consistency getConsistency() {

        return options.getConsistency();
    }
}
