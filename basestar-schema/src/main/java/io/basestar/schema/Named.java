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
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import io.basestar.util.Name;

import javax.annotation.Nonnull;
import java.io.IOException;

public interface Named {

    default String getName() {

        return getQualifiedName().last();
    }

    Name getQualifiedName();

    class QualifiedNameSerializer extends JsonSerializer<Named> {

        @Override
        public void serialize(final Named named, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider) throws IOException {

            jsonGenerator.writeString(named.getQualifiedName().toString());
        }

        @Override
        public void serializeWithType(final Named named, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {

            serialize(named, jsonGenerator, serializerProvider);
        }
    }

    // FIXME: temporary
    interface Qualified /*extends Named*/ {

        default String getSimpleName() {

            return getQualifiedName().last();
        }

        @Nonnull
        Name getQualifiedName();

        class Serializer extends JsonSerializer<Qualified> {

            @Override
            public void serialize(final Qualified qualifiedNamed, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider) throws IOException {

                jsonGenerator.writeString(qualifiedNamed.getQualifiedName().toString());
            }

            @Override
            public void serializeWithType(final Qualified qualifiedNamed, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {

                serialize(qualifiedNamed, jsonGenerator, serializerProvider);
            }
        }
    }
}
