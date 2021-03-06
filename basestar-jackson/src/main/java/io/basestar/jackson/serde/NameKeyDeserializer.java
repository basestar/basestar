package io.basestar.jackson.serde;

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

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.KeyDeserializer;
import io.basestar.util.Name;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class NameKeyDeserializer extends KeyDeserializer {

    private final boolean allowEmpty;

    public NameKeyDeserializer() {

        this(false);
    }

    @Override
    public Object deserializeKey(final String str, final DeserializationContext deserializationContext) {

        return allowEmpty ? Name.parse(str) : Name.parseNonEmpty(str);
    }
}
