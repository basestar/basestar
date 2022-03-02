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

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

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

    static final String INPUT =    "H4sIAAAAAAAAAM2QvU/DMBDFr6kEksdOMDIwValst/mUOnTJBlJFxcDmxJfWUh1XthsEfz1uChMwICTETfb5vXvnHxkDRK7ZoRZwAQAr59BDpCTchhtLapYXCY2TVMh4wXMRizovYl5nLF9kGBoSJj1ap0w3+E81rrE1FuFhBECMVVvViX2l9ngvNH6RRs6dx/cxUWNReJRwFRScchrTLGbphvGSp2XCZ5Sx4DFH2+DncOLF1gGRaFUvvOrRwcSpV4TRh2B4QllZoyHCIB+8N6cKY32I/tn/SWP0waI7L388yG+WT2jJ2CxNF0DaAKMyVgt/Pm9eDiF0J9wOroPvOE9b3T3ptZu2q3073d4V1D0vlwGtaD3avyV7+c/JzsskL1kRyPJfkIU3k871MQoDAAA=";
    // This string is an RFC 1952 stream encoded in Base64
    // RFC 1952 says:
    //          +---+---+---+---+---+---+---+---+---+---+
    //         |ID1|ID2|CM |FLG|     MTIME     |XFL|OS | (more-->)
    //         +---+---+---+---+---+---+---+---+---+---+
    /* for reference the list of "OS" values is:
         0 - FAT filesystem (MS-DOS, OS/2, NT/Win32)
         1 - Amiga
         2 - VMS (or OpenVMS)
         3 - Unix
         4 - VM/CMS
         5 - Atari TOS
         6 - HPFS filesystem (OS/2, NT)
         7 - Macintosh
         8 - Z-System
         9 - CP/M
        10 - TOPS-20
        11 - NTFS filesystem (NT)
        12 - QDOS
        13 - Acorn RISCOS
       255 - unknown
     */

    @Test
    void testGzipBson() {
        final byte[] inputBytes = BaseEncoding.base64().decode(INPUT);
        inputBytes[9] = (byte)0xFF; // 'unknown' OS

        final ObjectUpdatedEvent event = EventSerialization.gzipBson().deserialize(ObjectUpdatedEvent.class, inputBytes);
        assertEquals(Name.of("Asset"), event.getSchema());
        assertEquals("15b18950-56ad-428a-ab89-2b71847e28ad", event.getId());
        assertEquals(6L, event.getVersion());
        assertNotNull(event.getBefore());
        assertNotNull(event.getAfter());
        final byte[] outputBytes = EventSerialization.gzipBson().serialize(event);
        outputBytes[9] = (byte)0xFF; // 'unknown' OS
        final String output = BaseEncoding.base64().encode(outputBytes);

        assertEquals(hexEncode(inputBytes), hexEncode(outputBytes));

        // no matter what, the deserialized event still looks the same after a round-trip through our codec.
        final byte[] reinputBytes = BaseEncoding.base64().decode(output);
        final ObjectUpdatedEvent reEvent = EventSerialization.gzipBson().deserialize(ObjectUpdatedEvent.class, reinputBytes);
        assertEquals(event, reEvent);
    }

    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();
    static String hexEncode(byte[] bytes) {
        /* can't use JDK 17's HexFormat for now.
        * Can't just bytes.map(f"$_%02x").mkString(" ")
        * */
        char[] hexChars = new char[bytes.length * 3];
        for (int j = 0; j < bytes.length; ++j) {
            int v = bytes[j] & 0xFF;
            hexChars[j*3] = HEX_ARRAY[v >>> 4];
            hexChars[j*3 + 1] = HEX_ARRAY[v & 0x0F];
            hexChars[j*3 + 2] = ' ';
        }
        return new String(hexChars, 0, bytes.length*3 - 1);
    }



}
