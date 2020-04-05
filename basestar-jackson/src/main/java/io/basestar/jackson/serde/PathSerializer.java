//package io.basestar.jackson.serde;
//
//import com.fasterxml.jackson.core.JsonGenerator;
//import com.fasterxml.jackson.databind.JsonSerializer;
//import com.fasterxml.jackson.databind.SerializerProvider;
//import io.basestar.util.Path;
//
//import java.io.IOException;
//
//public class PathSerializer extends JsonSerializer<Path> {
//
//    @Override
//    public void serialize(final Path path, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider) throws IOException {
//
//        jsonGenerator.writeString(path.toString());
//    }
//}
