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
import io.basestar.database.event.ObjectUpdatedEvent;
import io.basestar.database.options.UpdateOptions;
import io.basestar.event.Event;
import io.basestar.expression.Context;
import io.basestar.schema.Instance;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.Permission;
import io.basestar.storage.exception.ObjectMissingException;
import io.basestar.storage.exception.VersionMismatchException;
import io.basestar.util.Nullsafe;
import io.basestar.util.Path;
import lombok.RequiredArgsConstructor;

import java.time.LocalDateTime;
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
    public Permission permission() {

        return schema.getPermission(Permission.UPDATE);
    }

    @Override
    public String id() {

        return options.getId();
    }

    @Override
    public Instance after(final Context context, final Instance before) {

        final String id = options.getId();

        if (before == null) {
            throw new ObjectMissingException(options.getSchema(), id);
        }

        if(!Instance.getSchema(before).equals(schema.getName())) {
            throw new IllegalStateException("Cannot change instance schema");
        }

        final Map<String, Object> data = new HashMap<>();
        final UpdateOptions.Mode mode = Nullsafe.option(options.getMode(), UpdateOptions.Mode.REPLACE);
        if(mode == UpdateOptions.Mode.MERGE) {
            data.putAll(before);
        } else if(mode != UpdateOptions.Mode.REPLACE) {
            throw new IllegalStateException();
        }
        if(options.getData() != null) {
            data.putAll(options.getData());
        }
        if(options.getExpressions() != null) {
            options.getExpressions().forEach((k, expr) -> data.put(k, expr.evaluate(context)));
        }
        final Map<String, Object> initial = new HashMap<>(schema.create(data));

        final LocalDateTime now = LocalDateTime.now();

        final Long beforeVersion = Instance.getVersion(before);
        assert beforeVersion != null;

        if(options.getVersion() != null && !beforeVersion.equals(options.getVersion())) {
            throw new VersionMismatchException(options.getSchema(), id, options.getVersion());
        }

        final Long afterVersion = beforeVersion + 1;

        Instance.setId(initial, id);
        Instance.setVersion(initial, afterVersion);
        Instance.setCreated(initial, Instance.getCreated(before));
        Instance.setUpdated(initial, now);
        Instance.setHash(initial, schema.hash(initial));

        final Instance evaluated = schema.evaluateProperties(context.with(CommonVars.VAR_THIS, initial), new Instance(initial));

        schema.validate(before, evaluated, context.with(CommonVars.VAR_THIS, evaluated));

        return evaluated;
    }

    @Override
    public Set<Path> afterExpand() {

        return options.getExpand();
    }

    @Override
    public Event event(final Instance before, final Instance after) {

        final String schema = Instance.getSchema(before);
        final String id = Instance.getId(before);
        final Long version = Instance.getVersion(before);
        assert version != null;
        return ObjectUpdatedEvent.of(schema, id, version, before, after);
    }

    @Override
    public Set<Path> paths() {

        // FIXME: shouldn't have to bind here, need to fix multi-part path constants in parser
        return Nullsafe.option(options.getExpressions()).values().stream()
                .flatMap(e -> e.bind(Context.init()).paths().stream())
                .collect(Collectors.toSet());
    }
}
