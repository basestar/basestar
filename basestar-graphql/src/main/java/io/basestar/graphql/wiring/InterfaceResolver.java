package io.basestar.graphql.wiring;

/*-
 * #%L
 * basestar-graphql
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

import graphql.TypeResolutionEnvironment;
import graphql.schema.GraphQLObjectType;
import graphql.schema.TypeResolver;
import io.basestar.schema.Instance;

import java.util.Map;

class InterfaceResolver implements TypeResolver {

    public static final InterfaceResolver INSTANCE = new InterfaceResolver();

    @Override
    public GraphQLObjectType getType(final TypeResolutionEnvironment env) {

        final Map<String, Object> object = env.getObject();
        return env.getSchema().getObjectType(Instance.getSchema(object));
    }
}
