package io.basestar.api;

import com.google.common.collect.ImmutableMap;
import io.basestar.auth.Caller;
import io.swagger.v3.oas.models.OpenAPI;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class TestAPIFormat {

    @Data
    public static class Input {

        private String string;

        private int integer;

        private double number;
    }

    @Data
    public static class Output {

        private Input input;
    }

    private API api() {

        return new API() {
            @Override
            public CompletableFuture<APIResponse> handle(final APIRequest request) throws IOException {

                final Input input = request.readBody(Input.class);

                final Output output = new Output();
                output.setInput(input);

                return CompletableFuture.completedFuture(APIResponse.response(request, 200, output));
            }

            @Override
            public CompletableFuture<OpenAPI> openApi() {

                return CompletableFuture.completedFuture(null);
            }
        };
    }

    private Input input() {

        final Input input = new Input();
        input.setInteger(10);
        input.setNumber(20.5);
        input.setString("test1");
        return input;
    }

    @Test
    public void testJsonFormat() throws IOException {

        testFormat(APIFormat.JSON);
    }

    @Test
    public void testYamlFormat() throws IOException {

        testFormat(APIFormat.YAML);
    }

    @Test
    public void testXmlFormat() throws IOException {

        testFormat(APIFormat.XML);
    }

    @Test
    public void testDefaultFormat() throws IOException {

        testFormat(APIFormat.JSON, Optional.empty());
    }

    @Test
    public void testMixedFormat() throws IOException {

        testFormat(APIFormat.JSON, Optional.of(APIFormat.YAML));
    }

    private void testFormat(final APIFormat format) throws IOException {

        testFormat(format, Optional.of(format));
    }

    private void testFormat(final APIFormat inputFormat, final Optional<APIFormat> outputFormat) throws IOException {

        final API api = api();
        final Input input = input();
        try (final ByteArrayOutputStream readInput = new ByteArrayOutputStream()) {
            inputFormat.getMapper().writeValue(readInput, input);
            final String inputStr = new String(readInput.toByteArray(), StandardCharsets.UTF_8);
            log.info("Input data: {}", inputStr);

            final Map<String, String> headers = outputFormat
                    .map(v -> ImmutableMap.of("Accept", v.getMimeType()))
                    .orElse(ImmutableMap.of());

            final APIResponse response = api.handle(APIRequest.request(Caller.SUPER, APIRequest.Method.POST, "/", ImmutableMap.of(),
                    headers, inputFormat, inputStr.getBytes(StandardCharsets.UTF_8))).join();

            outputFormat.ifPresent(v -> assertEquals(v, response.getContentType()));

            try (final ByteArrayOutputStream readOutput = new ByteArrayOutputStream()) {
                response.writeTo(readOutput);
                final String outputStr = new String(readOutput.toByteArray(), StandardCharsets.UTF_8);
                log.info("Output data: {}", outputStr);

                final Output output = response.getContentType().getMapper().readValue(outputStr, Output.class);
                assertEquals(input, output.getInput());
            }
        }
    }
}
