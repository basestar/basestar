package io.basestar.jackson.serde;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.basestar.jackson.BasestarModule;
import io.basestar.util.Name;
import io.basestar.util.Page;
import lombok.Value;
import org.apache.commons.io.IOUtils;
import org.json.JSONException;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class TestPageEnvelope {

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(BasestarModule.INSTANCE)
            .setSerializationInclusion(Include.NON_NULL);

    @Test
    public void testPageEnvelope() throws Exception {
        final Page<Dummy> page = aDummyPage();
        assertSer(page, "pageEnvelope.expected.json");
    }

    @Test
    public void testPageEnvelopeArray() throws Exception {
        final List<Page<Dummy>> pageList = singletonList(aDummyPage());
        assertSer(pageList, "pageEnvelopeArray.expected.json");
    }

    @Test
    public void testPageInPageEnvelope() throws Exception {
        final Page<Dummy> page = aDummyPage();

        final Page<Page<Dummy>> pageInPage = new Page<>(
                singletonList(page),
                new Page.Token("token999"),
                new Page.Stats(987L, 654L)
        );

        assertSer(pageInPage, "pageInPageEnvelope.expected.json");
    }

    @Test
    public void testPageEnvelopeNoStatsPaging() throws Exception {
        final Page<Dummy> page = Page.single(dummy3());
        assertSer(page, "pageEnvelopeNoStatsPaging.expected.json");
    }

    @Test
    public void testPageEnvelopeEmptyStats() throws Exception {
        final Page<Dummy> page = new Page<>(singletonList(dummy3()), null, Page.Stats.NULL);
        assertSer(page, "pageEnvelopeNoStatsPaging.expected.json");
    }

    @Test
    public void testEmptyPageEnvelope() throws Exception {
        final Page<Dummy> page = Page.empty();
        assertSer(page, "pageEmptyEnvelope.expected.json");
    }

    private Page<Dummy> aDummyPage() {
        return new Page<>(
                asList(
                        new Dummy(Name.parse("dummy1.dummy2"), asList("ld1", "ld2")),
                        dummy3()
                ),
                new Page.Token("token555"),
                new Page.Stats(123L, 456L)
        );
    }

    private Dummy dummy3() {
        return new Dummy(Name.parse("dummy3"), emptyList());
    }

    private void assertSer(final Object page, final String expectedFile) throws IOException, JSONException {
        final String pageOutput = objectMapper.writeValueAsString(page);

        final String expected = loadExpectedFile(expectedFile);

        JSONAssert.assertEquals(expected, pageOutput, true);
    }

    private String loadExpectedFile(final String expectedFile) throws IOException {
        final String relativePath = "page-envelope/" + expectedFile;
        final URL expectedUrl = Objects.requireNonNull(this.getClass().getResource(relativePath));
        return IOUtils.toString(expectedUrl, StandardCharsets.UTF_8);
    }

    @Value
    private static class Dummy {
        Name dummy1;
        List<String> dummyList;
    }

}
