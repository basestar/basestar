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
import io.basestar.schema.Instance;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.Permission;
import io.basestar.util.Path;

import java.util.Set;

public interface Action {

    ObjectSchema schema();

    String id();

    Permission permission();

//    RefKey beforeKey();

//    Set<Path> beforeCallerExpand();

    Instance after(Context context, Instance before);

    Set<Path> afterExpand();

    Event event(Instance before, Instance after);

    Set<Path> paths();

//    ExpandKey<RefKey> afterKey(Instance after);
//
//    Set<Path> afterCallerExpand();
}
