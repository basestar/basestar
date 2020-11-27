package io.basestar.jackson;

/*-
 * #%L
 * basestar-jackson
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

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TreeTraversingParser;
import com.google.common.collect.ImmutableList;
import io.basestar.util.Streams;
import lombok.experimental.Delegate;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BasestarFactory extends JsonFactory {

    public static final String INCLUDE = "@include";

    public static final String MERGE = "@merge";

    @Delegate(excludes = Excludes.class)
    private final JsonFactory delegate;

    public BasestarFactory() {

        this(new JsonFactory());
    }

    public BasestarFactory(final JsonFactory delegate) {

        this.delegate = delegate;
    }

    private JsonParser parser(final JsonParser parser, final URI uri) throws IOException {

        try {
            final JsonNode node = process((JsonNode) parser.readValueAsTree(), uri);
            return new TreeTraversingParser(node, parser.getCodec());
        } catch (final Exception e) {
            parser.close();
            throw e;
        }
    }

    private JsonNode process(final JsonNode node, final URI uri) throws IOException {

        if(node == null) {
            return null;
        } else if(node.isObject()) {
            return process((ObjectNode)node, uri);
        } else if(node.isArray()) {
            return process((ArrayNode)node, uri);
        } else {
            return node;
        }
    }

    private JsonNode process(final ObjectNode node, final URI uri) throws IOException {

        ObjectNode result = new ObjectNode(JsonNodeFactory.instance);
        final Iterator<Map.Entry<String, JsonNode>> iter = node.fields();
        while(iter.hasNext()) {
            final Map.Entry<String, JsonNode> entry = iter.next();
            if(!entry.getKey().startsWith("@")) {
                result.set(entry.getKey(), process(entry.getValue(), uri));
            }
        }
        final JsonNode merge = node.get(MERGE);
        if (merge != null) {
            final List<JsonNode> merges;
            if (merge.isArray()) {
                merges = ImmutableList.copyOf(merge);
            } else {
                merges = ImmutableList.of(merge);
            }

            for (final JsonNode v : merges) {
                if (v.isTextual()) {
                    for(final URI u : allUris(uri.resolve(merge.asText()))) {
                        final JsonNode mergeNode = createParser(u.toURL()).readValueAsTree();
                        if (mergeNode.isObject()) {
                            result = merge(result, (ObjectNode)mergeNode);
                        } else {
                            throw new IllegalStateException("@merge result must be an object");
                        }
                    }
                } else {
                    throw new IllegalStateException("@merge entries must be strings");
                }
            }
        }

        return result;
    }

    private Set<URI> allUris(final URI uri) throws IOException {

        if(uri.getScheme().equals("file")) {
            try (final Stream<Path> paths = Files.walk(Paths.get(uri))) {

                return paths.filter(Files::isRegularFile)
                        .map(Path::toUri).collect(Collectors.toSet());
            }
        } else {
            return Collections.singleton(uri);
        }
    }

    private JsonNode process(final ArrayNode node, final URI uri) throws IOException {

        final ArrayNode result = new ArrayNode(JsonNodeFactory.instance);
        for(final JsonNode a : node) {
            result.add(process(a, uri));
        }
        return result;
    }

    private ObjectNode merge(final ObjectNode a, final ObjectNode b) {

        final ObjectNode result = new ObjectNode(JsonNodeFactory.instance);
        Stream.concat(Streams.stream(a.fieldNames()), Streams.stream(b.fieldNames())).distinct().forEach(k -> {
            if(a.has(k) && b.has(k)) {
                final JsonNode vA = a.get(k);
                final JsonNode vB = b.get(k);
                if(vA.isObject() && vB.isObject()) {
                    result.set(k, merge((ObjectNode)vA, (ObjectNode)vB));
                } else {
                    result.set(k, a.get(k));
                }
            } else if(a.has(k)) {
                result.set(k, a.get(k));
            } else if(b.has(k)) {
                result.set(k, b.get(k));
            }
        });
        return result;
    }

    private URI workDir() {

        final Path currentRelativePath = Paths.get("");
        return currentRelativePath.toAbsolutePath().toUri();
    }

    @Override
    public JsonParser createParser(final File f) throws IOException {

        final URI uri = f.toURI();
        return parser(delegate.createParser(f), uri);
    }

    @Override
    public JsonParser createParser(final URL url) throws IOException {

        final URI uri;
        try {
            uri = url.toURI();
        } catch (final URISyntaxException e) {
            throw new IOException(e);
        }
        return parser(delegate.createParser(url), uri);
    }

    @Override
    public JsonParser createParser(final InputStream in) throws IOException {

        final URI uri = workDir();
        return parser(delegate.createParser(in), uri);
    }

    @Override
    public JsonParser createParser(final Reader r) throws IOException {

        final URI uri = workDir();
        return parser(delegate.createParser(r), uri);
    }

    @Override
    public JsonParser createParser(final byte[] data) throws IOException {

        final URI uri = workDir();
        return parser(delegate.createParser(data), uri);
    }

    @Override
    public JsonParser createParser(final byte[] data, final int offset, final int len) throws IOException {

        final URI uri = workDir();
        return parser(delegate.createParser(data, offset, len), uri);
    }

    @Override
    public JsonParser createParser(final String content) throws IOException {

        final URI uri = workDir();
        return parser(delegate.createParser(content), uri);
    }

    @Override
    public JsonParser createParser(final char[] content) throws IOException {

        final URI uri = workDir();
        return parser(delegate.createParser(content), uri);
    }

    @Override
    public JsonParser createParser(final char[] content, final int offset, final int len) throws IOException {

        final URI uri = workDir();
        return parser(delegate.createParser(content, offset, len), uri);
    }

    @Override
    public JsonParser createParser(final DataInput in) throws IOException {

        final URI uri = workDir();
        return parser(delegate.createParser(in), uri);
    }

    @SuppressWarnings("unused")
    private interface Excludes {

        JsonParser createParser(final File f) throws IOException;

        JsonParser createParser(final URL url) throws IOException;

        JsonParser createParser(final InputStream in) throws IOException;

        JsonParser createParser(Reader r) throws IOException;

        JsonParser createParser(final byte[] data) throws IOException;

        JsonParser createParser(final byte[] data, final int offset, final int len) throws IOException;

        JsonParser createParser(final String content) throws IOException;

        JsonParser createParser(final char[] content) throws IOException;

        JsonParser createParser(final char[] content, final int offset, final int len) throws IOException;

        JsonParser createParser(final DataInput in) throws IOException;

        // These methods are final in JsonFactory so can't be delegated

        JsonFactory configure(JsonFactory.Feature f, boolean state);

        JsonFactory configure(JsonParser.Feature f, boolean state);

        boolean isEnabled(JsonFactory.Feature f);

        boolean isEnabled(StreamReadFeature f);

        boolean isEnabled(StreamWriteFeature f);

        boolean isEnabled(JsonParser.Feature f);

        boolean isEnabled(JsonGenerator.Feature f);

        JsonFactory configure(JsonGenerator.Feature f, boolean state);

        int getParserFeatures();

        int getGeneratorFeatures();
    }
}
