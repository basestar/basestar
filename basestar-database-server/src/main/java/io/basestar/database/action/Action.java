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

import io.basestar.event.Event;
import io.basestar.expression.Context;
import io.basestar.schema.Consistency;
import io.basestar.schema.Instance;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.Permission;
import io.basestar.schema.util.ValueContext;
import io.basestar.storage.exception.UnsupportedWriteException;
import io.basestar.util.Name;

import java.util.Set;

public interface Action {

    ObjectSchema schema();

    String id();

    Permission permission(Instance before);

    Instance after(ValueContext valueContext, Context expressionContext, Instance before);

    Set<Name> afterExpand();

    Event event(Instance before, Instance after);

    Set<Name> paths();

    Consistency getConsistency();

    default void validate() {

        final ObjectSchema schema = schema();
        if(schema.isReadonly()) {
            throw new UnsupportedWriteException(schema.getQualifiedName(), "schema is readonly");
        }
    }
}
