package io.basestar.stream;

/*-
 * #%L
 * basestar-stream
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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.basestar.auth.Caller;
import io.basestar.expression.Expression;
import io.basestar.util.Path;
import lombok.Data;

import java.util.List;
import java.util.Set;

@Data
public class Subscription {

    private String id;

    private String sub;

    private String channel;

    @JsonDeserialize(builder = Caller.Builder.class)
    private Caller caller;

    private Expression expression;

    private Set<Path> expand;

    @Data
    public static class Key {

        private final String schema;

        private final String index;

        private final List<Object> partition;
    }
}
