//package io.basestar.jackson.encoding;

/*-
 * #%L
 * basestar-jackson
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2020 basestar.io
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
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.google.common.io.BaseEncoding;
//import de.undercouch.bson4jackson.BsonFactory;
//import io.basestar.encoding.Encoding;
//
//import java.io.ByteArrayInputStream;
//import java.io.ByteArrayOutputStream;
//import java.io.IOException;
//
//public class BsonEncoding<I> implements Encoding<I, String> {
//
//    private static final ObjectMapper objectMapper = new ObjectMapper(new BsonFactory());
//
//    private static final BaseEncoding encoding = BaseEncoding.base64();
//
//    @Override
//    public String encode(final I v) {
//
//        try(final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
//            objectMapper.writeValue(baos, v);
//            return encoding.encode(baos.toByteArray());
//        } catch(final IOException e) {
//            throw new IllegalStateException(e);
//        }
//    }
//
//    @Override
//    public I decode(final Class<I> cls, final String v) {
//
//        final byte[] bytes = encoding.decode(str);
//        try(final ByteArrayInputStream bais = new ByteArrayInputStream(bytes)) {
//            return objectMapper.readValue(bais, Event.class);
//        } catch(final IOException e) {
//            throw new IllegalStateException(e);
//        }
//    }
//}
