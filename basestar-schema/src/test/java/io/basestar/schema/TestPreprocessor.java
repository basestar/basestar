//package io.basestar.schema;
//
//import com.fasterxml.jackson.databind.JsonNode;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import org.junit.jupiter.api.Test;
//
//import java.io.File;
//import java.io.IOException;
//
//public class TestPreprocessor {
//
//    @Test
//    public void testInclude() throws IOException {
//
//        final ObjectMapper mapper = new ObjectMapper(new BasestarFactory());
//        final JsonNode node = mapper.readTree(new File("../basestar-schema/src/test/resources/test.json"));
//        System.err.println(node);
//    }
//}
