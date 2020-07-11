package io.basestar.mapper.internal;

/*-
 * #%L
 * basestar-mapper
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
import io.basestar.mapper.MappingContext;
import io.basestar.schema.ViewSchema;
import io.basestar.type.TypeContext;
import io.basestar.util.Name;

public class ViewSchemaMapper<T> extends InstanceSchemaMapper<T, ViewSchema.Builder> {

    private final Name from;

    private final Expression where;

    public ViewSchemaMapper(final MappingContext context, final Name name, final TypeContext type, final Name from, final Expression where) {

        super(context, name, type, ViewSchema.Builder.class);
        this.from = from;
        this.where = where;
    }

    @Override
    public ViewSchema.Builder schema() {

        return addMembers(ViewSchema.builder().setFrom(from).setWhere(where));
    }
}
