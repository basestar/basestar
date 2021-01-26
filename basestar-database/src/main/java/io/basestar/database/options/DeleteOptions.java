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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import io.basestar.auth.Caller;
import io.basestar.database.Database;
import io.basestar.schema.Consistency;
import io.basestar.util.Name;
import lombok.Builder;
import lombok.Data;

import java.util.concurrent.CompletableFuture;

@Data
@Builder(toBuilder = true, builderClassName = "Builder", setterPrefix = "set")
@JsonDeserialize(builder = DeleteOptions.Builder.class)
public class DeleteOptions implements ActionOptions {

    public static final String TYPE = "delete";

    private final Name schema;

    private final String id;

    private final Long version;

    private final Consistency consistency;

    @Override
    public CompletableFuture<?> apply(final Caller caller, final Database database) {

        return database.delete(caller, this);
    }

    @JsonPOJOBuilder(withPrefix = "set")
    public static class Builder {

    }
}
