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

import com.google.common.io.BaseEncoding;
import io.basestar.util.Name;
import lombok.Data;
import lombok.experimental.Accessors;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TestEventSerialization {

    @Data
    @Accessors(chain = true)
    static class ObjectUpdatedEvent implements Event {

        private Name schema;

        private String id;

        private long version;

        private Map<String, Object> before;

        private Map<String, Object> after;

        @Override
        public Event abbreviate() {

            return this;
        }
    }

    @Test
    void testGzipBson() {

        final String input = "H4sIAAAAAAAAAM2QvU/DMBDFr6kEksdOMDIwValst/mUOnTJBlJFxcDmxJfWUh1XthsEfz1uChMwICTETfb5vXvnHxkDRK7ZoRZwAQAr59BDpCTchhtLapYXCY2TVMh4wXMRizovYl5nLF9kGBoSJj1ap0w3+E81rrE1FuFhBECMVVvViX2l9ngvNH6RRs6dx/cxUWNReJRwFRScchrTLGbphvGSp2XCZ5Sx4DFH2+DncOLF1gGRaFUvvOrRwcSpV4TRh2B4QllZoyHCIB+8N6cKY32I/tn/SWP0waI7L388yG+WT2jJ2CxNF0DaAKMyVgt/Pm9eDiF0J9wOroPvOE9b3T3ptZu2q3073d4V1D0vlwGtaD3avyV7+c/JzsskL1kRyPJfkIU3k871MQoDAAA=";
        final byte[] inputBytes = BaseEncoding.base64().decode(input);
        final ObjectUpdatedEvent event = EventSerialization.gzipBson().deserialize(ObjectUpdatedEvent.class, inputBytes);
        assertEquals(Name.of("Asset"), event.getSchema());
        assertEquals("15b18950-56ad-428a-ab89-2b71847e28ad", event.getId());
        assertEquals(6L, event.getVersion());
        assertNotNull(event.getBefore());
        assertNotNull(event.getAfter());
        final byte[] outputBytes = EventSerialization.gzipBson().serialize(event);
        final String output = BaseEncoding.base64().encode(outputBytes);
        assertEquals(input, output);
    }
}
