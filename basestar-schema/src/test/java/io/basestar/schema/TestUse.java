package io.basestar.schema;

/*-
 * #%L
 * basestar-schema
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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.basestar.schema.use.*;
import io.leangen.geantyref.TypeToken;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestUse {

    @Test
    void deserialize() throws IOException {

        final Use<?> string = new ObjectMapper().readValue("\"string\"", Use.class);
        assertEquals(UseString.DEFAULT, string);

        final Use<?> array = new ObjectMapper().readValue("{\"array\": \"string\"}", Use.class);
        assertEquals(new UseArray<>(UseString.DEFAULT), array);

        final Use<?> map = new ObjectMapper().readValue("{\"map\": \"string\"}", Use.class);
        assertEquals(new UseMap<>(UseString.DEFAULT), map);
    }

    @Test
    void fromType() {

        final Use<?> intMap = Use.fromType((new TypeToken<Map<String, Integer>>() {}).getType());
        assertEquals(UseMap.from(UseInteger.DEFAULT), intMap);

        final Use<?> anyMap = Use.fromType(Map.class);
        assertEquals(UseMap.from(UseAny.DEFAULT), anyMap);
    }
}
