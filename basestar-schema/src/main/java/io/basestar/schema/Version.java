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


import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.basestar.schema.exception.SchemaValidationException;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.util.Arrays;

@Getter
@RequiredArgsConstructor
@JsonSerialize(using = Version.Serializer.class)
@JsonDeserialize(using = Version.Deserializer.class)
public enum Version {

    V_2020_08_04("2020-08-04"),
    LEGACY("legacy");

    public static final Version CURRENT = LEGACY;

    private final String id;

    public static Version fromString(final String str) {

        if("LATEST".equalsIgnoreCase(str)) {
            return V_2020_08_04;
        } else {
            return Arrays.stream(Version.values()).filter(v -> v.id.equals(str))
                    .findFirst().orElseThrow(() -> new SchemaValidationException("Version " + str + " is not supported"));
        }
    }

    @Override
    public String toString() {

        return this.getId();
    }

    public static class Deserializer extends JsonDeserializer<Version> {

        @Override
        public Version deserialize(final JsonParser parser, final DeserializationContext context) throws IOException {

            return Version.fromString(parser.getValueAsString());
        }
    }

    public static class Serializer extends JsonSerializer<Version> {

        @Override
        public void serialize(final Version version, final JsonGenerator generator, final SerializerProvider provider) throws IOException {

            generator.writeString(version.getId());
        }
    }
}
