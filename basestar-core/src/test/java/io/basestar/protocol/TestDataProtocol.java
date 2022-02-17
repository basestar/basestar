package io.basestar.protocol;

import com.google.common.io.CharStreams;
import io.basestar.Basestar;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestDataProtocol {

    @Test
    public void testDataProtocol() throws IOException {

        Basestar.init();

        final String data = "{\"hello\": \"world\"}";

        final URL url = new URL("data:application/json;base64," + Base64.getEncoder().encodeToString(data.getBytes(StandardCharsets.UTF_8)));
        try (final InputStream is = url.openStream();
             final Reader reader = new InputStreamReader(is)) {
            final String result = CharStreams.toString(reader);
            assertEquals(data, result);
        }
    }
}
