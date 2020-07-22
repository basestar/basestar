package io.basestar.jackson;

/*-
 * #%L
 * basestar-jackson
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

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import io.basestar.expression.Expression;
import io.basestar.jackson.serde.*;
import io.basestar.util.Name;
import io.basestar.util.PagingToken;
import io.basestar.util.Sort;

public class BasestarModule extends SimpleModule {

    public BasestarModule() {

        super("Basestar", new Version(1, 0, 0, null, null, null));

        final ToStringSerializer toString = new ToStringSerializer();

        addSerializer(Name.class, toString);
        addDeserializer(Name.class, new NameDeserializer());

        addKeySerializer(Name.class, toString);
        addKeyDeserializer(Name.class, new NameKeyDeserializer());

        addSerializer(Sort.class, toString);
        addDeserializer(Sort.class, new SortDeserializer());

        addSerializer(Expression.class, toString);
        addDeserializer(Expression.class, new ExpressionDeserializer());

        addSerializer(PagingToken.class, toString);
        addDeserializer(PagingToken.class, new PagingTokenDeserializer());
    }
}
