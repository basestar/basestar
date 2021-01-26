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
import com.google.common.collect.ImmutableMap;
import io.basestar.auth.Caller;
import io.basestar.database.Database;
import io.basestar.schema.Consistency;
import io.basestar.util.Nullsafe;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Data
@Accessors(chain = true)
@JsonDeserialize(builder = BatchOptions.Builder.class)
public class BatchOptions implements Options {

    public static final String TYPE = "batch";

    private final Consistency consistency;

    private final Map<String, ActionOptions> actions;

    private BatchOptions(final Builder builder) {

        this.consistency = Nullsafe.orDefault(builder.consistency, Consistency.EVENTUAL);
        this.actions = ImmutableMap.copyOf(builder.actions);
    }

    public static Builder builder() {

        return new Builder();
    }

    @Override
    public CompletableFuture<?> apply(final Caller caller, final Database database) {

        return database.batch(caller, this);
    }

    @JsonPOJOBuilder(withPrefix = "set")
    public static class Builder {

        private Consistency consistency;

        private final LinkedHashMap<String, ActionOptions> actions = new LinkedHashMap<>();

        public Builder setConsistency(final Consistency consistency) {

            this.consistency = consistency;
            return this;
        }

        public Builder putAction(final String name, final ActionOptions action) {

            this.actions.put(name, action);
            return this;
        }

        public Builder setActions(final Map<String, ActionOptions> actions) {

            this.actions.putAll(actions);
            return this;
        }

        public BatchOptions build() {

            return new BatchOptions(this);
        }
    }
}
