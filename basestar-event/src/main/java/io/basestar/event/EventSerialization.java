package io.basestar.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.undercouch.bson4jackson.BsonFactory;

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

        private static final ObjectMapper objectMapper = new ObjectMapper(new BsonFactory());

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
