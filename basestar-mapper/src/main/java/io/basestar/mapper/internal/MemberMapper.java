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
import io.basestar.schema.InstanceSchema;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Set;

public interface MemberMapper<B extends InstanceSchema.Builder> {

    TypeMapper getType();

    void addToSchema(InstanceSchemaMapper<?, B> mapper, B builder);

    void unmarshall(Object source, Map<String, Object> target) throws InvocationTargetException, IllegalAccessException;

    void marshall(Map<String, Object> source, Object target) throws InvocationTargetException, IllegalAccessException;

    MemberMapper<B> withExpression(Expression expression);

    default Set<Class<?>> dependencies() {

        return getType().dependencies();
    }
}
