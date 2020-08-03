package io.basestar.database.options;

/*-
 * #%L
 * basestar-database
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

import io.basestar.expression.Expression;
import io.basestar.schema.Consistency;
import io.basestar.util.Name;
import lombok.Builder;
import lombok.Data;

import java.util.Map;
import java.util.Set;

@Data
@Builder(toBuilder = true, builderClassName = "Builder")
public class CreateOptions implements ActionOptions {

    public static final String TYPE = "create";

    private final Name schema;

    private final String id;

    private final Map<String, Object> data;

    private final Consistency consistency;

    private final Map<String, Expression> expressions;

    private final Set<Name> expand;

    private final Set<Name> projection;
}
