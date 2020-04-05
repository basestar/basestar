//package io.basestar.jackson.encoding;
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
