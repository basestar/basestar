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


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.basestar.schema.exception.SchemaValidationException;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Arrays;

@Getter
@RequiredArgsConstructor
public enum Version {

    V_2020_08_04("2020-08-04"),
    LEGACY("legacy");

    public static final Version CURRENT = LEGACY;

    private final String id;

    @JsonCreator
    public static Version fromString(final String str) {

        if("LATEST".equalsIgnoreCase(str)) {
            return V_2020_08_04;
        } else {
            return Arrays.stream(Version.values()).filter(v -> v.id.equals(str))
                    .findFirst().orElseThrow(() -> new SchemaValidationException("Version " + str + " is not supported"));
        }
    }

    @JsonValue
    @Override
    public String toString() {

        return this.getId();
    }
}
