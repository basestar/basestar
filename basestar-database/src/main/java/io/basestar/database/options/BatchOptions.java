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

import com.google.common.collect.ImmutableMap;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.LinkedHashMap;
import java.util.Map;

@Data
@Accessors(chain = true)
public class BatchOptions implements Options {

    public static final String TYPE = "batch";

    private final Map<String, ActionOptions> actions;

    private BatchOptions(final Builder builder) {

        this.actions = ImmutableMap.copyOf(builder.actions);
    }

    public static Builder builder() {

        return new Builder();
    }

    public static class Builder {

        private final LinkedHashMap<String, ActionOptions> actions = new LinkedHashMap<>();

        public Builder action(final String name, final ActionOptions action) {

            this.actions.put(name, action);
            return this;
        }

        public Builder actions(final Map<String, ActionOptions> actions) {

            this.actions.putAll(actions);
            return this;
        }

        public BatchOptions build() {

            return new BatchOptions(this);
        }
    }

//    @Data
//    @lombok.Builder
//    public static class Action {
//
//        private final CreateOptions create;
//
//        private final UpdateOptions update;
//
//        private final DeleteOptions delete;
//
//        private Action(final CreateOptions create, final UpdateOptions update, final DeleteOptions delete) {
//
//            if(Stream.of(create, update, delete).filter(Objects::nonNull).count() != 1) {
//                throw new IllegalStateException("Must specify one of [create, update, delete]");
//            }
//            this.create = create;
//            this.update = update;
//            this.delete = delete;
//        }
//
//
//        public static Action create(final CreateOptions create) {
//
//            return new Action(create, null, null);
//        }
//
//        public static Action update(final UpdateOptions update) {
//
//            return new Action(null, update, null);
//        }
//
//        public static Action delete(final DeleteOptions delete) {
//
//            return new Action(null, null, delete);
//        }
//    }
}
