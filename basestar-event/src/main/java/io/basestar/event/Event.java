package io.basestar.event;

/*-
 * #%L
 * basestar-event
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

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Collections;
import java.util.Map;

public interface Event {

    Event abbreviate();

    /**
     * With default serialization/deserialization this must be the FQN of the event class
     */

    @JsonIgnore
    default String eventType() {

        return this.getClass().getName();
    }

    /**
     * Module is only used for routing, default is to use the package of the event class with 'event' removed if it is
     * the last package name element.
     * <p>
     * e.g. the module of io.basestar.custom.event.MyEvent will be io.basestar.custom
     */

    @JsonIgnore
    default String eventModule() {

        final String eventPackage = this.getClass().getPackage().getName();
        return eventPackage.replaceAll("\\.event$", "");
    }

    @JsonIgnore
    default Map<String, String> eventMetadata() {

        return Collections.emptyMap();
    }
}
