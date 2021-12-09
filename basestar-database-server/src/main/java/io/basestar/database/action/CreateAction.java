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
import io.basestar.database.options.CreateOptions;
import io.basestar.event.Event;
import io.basestar.expression.Context;
import io.basestar.schema.*;
import io.basestar.schema.exception.ConstraintViolationException;
import io.basestar.schema.util.ValueContext;
import io.basestar.storage.exception.ObjectExistsException;
import io.basestar.util.ISO8601;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import lombok.RequiredArgsConstructor;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class CreateAction implements Action {

    private final ObjectSchema schema;

    private final CreateOptions options;

    @Override
    public ObjectSchema schema() {

        return schema;
    }

    @Override
    public Permission permission(final Instance before) {

        return schema.getPermission(Permission.CREATE);
    }

    @Override
    public String id() {

        return options.getId();
    }

    @Override
    public Result after(final ValueContext valueContext, final Context expressionContext, final Instance before) {

        if (before != null) {
            throw new ObjectExistsException(schema.getQualifiedName(), Instance.getId(before));
        }

        final Map<String, Object> data = new HashMap<>();
        if (options.getData() != null) {
            data.putAll(options.getData());
        }
        if (options.getExpressions() != null) {
            options.getExpressions().forEach((k, expr) -> data.put(k, expr.evaluate(expressionContext)));
        }

        final Map<String, Object> initial = new HashMap<>(schema.create(valueContext, data, null));

        final Instant now = ISO8601.now();

        // FIXME: split validation so that required validation can be applied here
        // FIXME: or make evaluation (including id evaluation) respect dependencies

        // Id in path overrides id specified in body
        final String requestedId = options.getId() == null ? Instance.getId(initial) : options.getId();

        final String actualId;
        if (schema.getId() != null) {
            actualId = schema.getId().evaluate(requestedId, expressionContext.with(CommonVars.VAR_THIS, initial));
        } else if (requestedId != null) {
            actualId = requestedId;
        } else {
            actualId = UUID.randomUUID().toString();
        }

        Instance.setId(initial, actualId);
        Instance.setVersion(initial, 1L);
        Instance.setCreated(initial, now);
        Instance.setUpdated(initial, now);
        Instance.setHash(initial, schema.hash(initial));

        final Instance evaluated = schema.evaluateProperties(expressionContext.with(CommonVars.VAR_THIS, initial), new Instance(initial), Collections.emptySet());

        final Set<Constraint.Violation> violations = schema.validate(expressionContext.with(CommonVars.VAR_THIS, evaluated), evaluated, evaluated);
        if (!violations.isEmpty()) {
            throw new ConstraintViolationException(violations);
        }

        return new Result(Result.Type.CREATE, evaluated);
    }

    @Override
    public Set<Name> afterExpand() {

        return options.getExpand();
    }

    @Override
    public Event event(final Instance before, final Instance after) {

        final Name schema = Instance.getSchema(after);
        final String id = Instance.getId(after);
        return ObjectCreatedEvent.of(schema, id, after);
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
