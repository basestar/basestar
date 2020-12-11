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

import com.fasterxml.jackson.databind.ObjectMapper;
import de.undercouch.bson4jackson.BsonFactory;
import io.basestar.jackson.BasestarModule;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public interface EventSerialization {

    byte[] serialize(Event event);

    <E extends Event> E deserialize(Class<E> event, byte[] bytes);

    static EventSerialization gzipBson() {

        return GzipBson.INSTANCE;
    }

    class GzipBson implements EventSerialization {

        public static final EventSerialization INSTANCE = new GzipBson();

        private static final ObjectMapper objectMapper = new ObjectMapper(new BsonFactory())
                .registerModule(BasestarModule.INSTANCE);

        @Override
        public byte[] serialize(final Event event) {

            try(final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final GZIPOutputStream gzos = new GZIPOutputStream(baos)) {
                objectMapper.writeValue(gzos, event);
                return baos.toByteArray();
            } catch(final IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public <E extends Event> E deserialize(final Class<E> event, final byte[] bytes) {

            try(final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                final GZIPInputStream gzis = new GZIPInputStream(bais)) {
                return objectMapper.readValue(gzis, event);
            } catch(final IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
