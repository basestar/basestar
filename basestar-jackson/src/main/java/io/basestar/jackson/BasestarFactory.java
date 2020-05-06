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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TreeTraversingParser;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
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

    /*private static class ParserImpl extends ParserBase {

        private final JsonFactory factory;

        private final JsonParser root;

        private final URL url;

        private Queue<JsonParser> delegates = new LinkedList<>();

        private JsonToken container;

        private JsonToken buffer;

        public ParserImpl(final JsonFactory factory, final JsonParser delegate, final URL url) {

            this.factory = factory;
            this.root = delegate;
            this.url = url;
            final JsonNode node = delegate.readValueAsTree();

        }

//        @Delegate(excludes = Excludes.class)
//        public JsonParser delegate() {
//
//            if(delegates.isEmpty()) {
//                return root;
//            } else {
//                return delegates.peek();
//            }
//        }

        @Override
        public JsonToken nextToken() throws IOException {

            final JsonToken token = nextTokenImpl();
            if(url.getPath().endsWith("test.json")) {
                System.err.println(token);
                if(token.equals(JsonToken.VALUE_STRING)) {
                    System.err.println("here");
                }
            } else {
                System.err.println("(" + token + ")");
            }
            return token;
        }

        @Override
        protected void _handleEOF() throws JsonParseException {

        }

        @Override
        public String getCurrentName() throws IOException {
            return null;
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public JsonStreamContext getParsingContext() {
            return null;
        }

        @Override
        public void overrideCurrentName(final String s) {

        }

        @Override
        public String getText() throws IOException {
            return null;
        }

        @Override
        public char[] getTextCharacters() throws IOException {
            return new char[0];
        }

        @Override
        public boolean hasTextCharacters() {
            return false;
        }

        @Override
        public int getTextLength() throws IOException {
            return 0;
        }

        @Override
        public int getTextOffset() throws IOException {
            return 0;
        }

        @Override
        public byte[] getBinaryValue(final Base64Variant base64Variant) throws IOException {
            return new byte[0];
        }

        public JsonToken nextTokenImpl() throws IOException {

            if(!delegates.isEmpty()) {
                System.err.println("Read from delegate");
                final JsonParser delegate = delegates.peek();
                assert delegate != null;
                final JsonStreamContext context = delegate.getParsingContext();
                final boolean wasRoot = context.inRoot();
                final JsonToken token = delegate.nextToken();
                System.err.println("t:" + token);
                if(token == null || token == JsonToken.NOT_AVAILABLE) {
                    System.err.println("Close delegate");
                    delegates.remove();
//                        token = nextToken();
                    return nextTokenImpl();
                }
//                } while(token == null);

//                if(token == null || (token == JsonToken.END_OBJECT && wasRoot)) {
//                    delegates.remove();
//                    return nextToken();
//                } else if(token == JsonToken.START_OBJECT && context.inRoot()) {
//                    return nextToken();
//                } else {
                    return token;
//                }
            } else if (buffer == null) {
                final JsonToken token = root.nextToken();
                if (token == JsonToken.START_OBJECT) {
                    JsonToken next = root.nextToken();
                    if (next == JsonToken.FIELD_NAME && root.getCurrentName().equals("@include")) {
                        next = root.nextToken();
                        assert (next == JsonToken.VALUE_STRING);
                        final String path = root.getText();
                        final URL url = new URL(this.url, path);
                        delegates.add(factory.createParser(url));
                        container = null;
//                        final JsonToken skip = root.nextToken();
//                        if(skip != JsonToken.END_OBJECT) {
//                            throw new IllegalStateException("Expected end-of-object");
//                        }
                        return nextTokenImpl();
                    } else {
                        buffer = next;
                        return token;
                    }
//                } else if(token == null && !delegates.isEmpty()) {
//                    delegates.remove();
//                    return nextToken();
                } else {
                    return token;
                }
            } else {
                final JsonToken result = buffer;
                buffer = null;
                return result;
            }
        }

//        @Override
//        public Object getCurrentValue() {
//
//            return delegate().getCurrentValue();
//        }
//
//        @Override
//        public void setCurrentValue(final Object v) {
//
//            delegate().setCurrentValue(v);
//        }
//
//        @Override
//        public void setCodec(final ObjectCodec c) {
//
//            delegate().setCodec(c);
//        }
//
//        @Override
//        public ObjectCodec getCodec() {
//
//            return delegate().getCodec();
//        }
//
//        @Override
//        public JsonParser enable(final Feature f) {
//
//            delegate().enable(f);
//            return this;
//        }
////
//        @Override
//        public JsonParser disable(final Feature f) {
//
//            delegate().disable(f);
//            return this;
//        }
//
//        @Override
//        public boolean isEnabled(final Feature f) {
//
//            return delegate().isEnabled(f);
//        }
//
//        @Override
//        public int getFeatureMask() {
//
//            return delegate().getFeatureMask();
//        }
//
//        @Deprecated
//        @Override
//        public JsonParser setFeatureMask(final int mask) {
//
//            delegate().setFeatureMask(mask);
//            return this;
//        }
//
//        @Override
//        public JsonParser overrideStdFeatures(final int values, final int mask) {
//
//            delegate().overrideStdFeatures(values, mask);
//            return this;
//        }
//
//        @Override
//        public JsonParser overrideFormatFeatures(final int values, final int mask) {
//
//            delegate().overrideFormatFeatures(values, mask);
//            return this;
//        }
//
//        @Override
//        public FormatSchema getSchema() {
//
//            return delegate().getSchema();
//        }
//
//        @Override
//        public void setSchema(FormatSchema schema) {
//
//
//            delegate().setSchema(schema);
//        }
//
//        @Override
//        public boolean canUseSchema(final FormatSchema schema) {
//
//            return delegate().canUseSchema(schema);
//        }
//
//        @Override
//        public Version version() {
//
//            return delegate().version();
//        }
//
//        @Override
//        public Object getInputSource() {
//
//            return delegate().getInputSource();
//        }
//
//        @Override
//        public boolean requiresCustomCodec() {
//
//            return delegate().requiresCustomCodec();
//        }
//
//        @Override
//        public void close() throws IOException {
//
//            delegate().close();
//        }
//
//        @Override
//        public boolean isClosed() {
//
//            return delegate().isClosed();
//        }
//
//        @Override
//        public JsonToken currentToken() {
//
//            return delegate().currentToken();
//        }
//
//        @Override
//        public int currentTokenId() {
//
//            return delegate().currentTokenId();
//        }
//
//        @Override
//        public JsonToken getCurrentToken() {
//
//            return delegate().getCurrentToken();
//        }
//
//        @Override
//        public int getCurrentTokenId() {
//
//            return delegate().getCurrentTokenId();
//        }
//
//        @Override
//        public boolean hasCurrentToken() {
//
//            return delegate().hasCurrentToken();
//        }
//
//        @Override
//        public boolean hasTokenId(int id) {
//
//            return delegate().hasTokenId(id);
//        }
//
//        @Override
//        public boolean hasToken(JsonToken t) {
//
//            return delegate().hasToken(t);
//        }
//
//        @Override
//        public String getCurrentName() throws IOException {
//
//            return delegate().getCurrentName();
//        }
//
//        @Override
//        public JsonLocation getCurrentLocation() {
//
//            return delegate().getCurrentLocation();
//        }
//
//        @Override
//        public JsonStreamContext getParsingContext() {
//
//            return delegate().getParsingContext();
//        }
//
//        @Override
//        public boolean isExpectedStartArrayToken() {
//
//            return delegate().isExpectedStartArrayToken();
//        }
//
//        @Override
//        public boolean isExpectedStartObjectToken() {
//
//            return delegate().isExpectedStartObjectToken();
//        }
//
//        @Override
//        public boolean isNaN() throws IOException {
//
//            return delegate().isNaN();
//        }
//
//        @Override
//        public void clearCurrentToken() {
//
//            delegate().clearCurrentToken();
//        }
//
//        @Override
//        public JsonToken getLastClearedToken() {
//
//            return delegate().getLastClearedToken();
//        }
//
//        @Override
//        public void overrideCurrentName(final String name) {
//
//            delegate().overrideCurrentName(name);
//        }
//
//        @Override
//        public String getText() throws IOException {
//
//            return delegate().getText();
//        }
//
//        @Override
//        public boolean hasTextCharacters() {
//
//            return delegate().hasTextCharacters();
//        }
//
//        @Override
//        public char[] getTextCharacters() throws IOException {
//
//            return delegate().getTextCharacters();
//        }
//
//        @Override
//        public int getTextLength() throws IOException {
//
//            return delegate().getTextLength();
//        }
//
//        @Override
//        public int getTextOffset() throws IOException {
//
//            return delegate().getTextOffset();
//        }
//
//        @Override
//        public int getText(final Writer writer) throws IOException, UnsupportedOperationException {
//
//            return delegate().getText(writer);
//        }
//
//        @Override
//        public BigInteger getBigIntegerValue() throws IOException {
//
//            return delegate().getBigIntegerValue();
//        }
//
//        @Override
//        public boolean getBooleanValue() throws IOException {
//
//            return delegate().getBooleanValue();
//        }
//
//        @Override
//        public byte getByteValue() throws IOException {
//
//            return delegate().getByteValue();
//        }
//
//        @Override
//        public short getShortValue() throws IOException {
//
//            return delegate().getShortValue();
//        }
//
//        @Override
//        public BigDecimal getDecimalValue() throws IOException {
//
//            return delegate().getDecimalValue();
//        }
//
//        @Override
//        public double getDoubleValue() throws IOException {
//
//            return delegate().getDoubleValue();
//        }
//
//        @Override
//        public float getFloatValue() throws IOException {
//
//            return delegate().getFloatValue();
//        }
//
//        @Override
//        public int getIntValue() throws IOException {
//
//            return delegate().getIntValue();
//        }
//
//        @Override
//        public long getLongValue() throws IOException {
//
//            return delegate().getLongValue();
//        }
//
//        @Override
//        public NumberType getNumberType() throws IOException {
//
//            return delegate().getNumberType();
//        }
//
//        @Override
//        public Number getNumberValue() throws IOException {
//
//            return delegate().getNumberValue();
//        }
//
//        @Override
//        public int getValueAsInt() throws IOException {
//
//            return delegate().getValueAsInt();
//        }
//
//        @Override
//        public int getValueAsInt(final int defaultValue) throws IOException {
//
//            return delegate().getValueAsInt(defaultValue);
//        }
//
//        @Override
//        public long getValueAsLong() throws IOException {
//
//            return delegate().getValueAsLong();
//        }
//
//        @Override
//        public long getValueAsLong(final long defaultValue) throws IOException {
//
//            return delegate().getValueAsLong(defaultValue);
//        }
//
//        @Override
//        public double getValueAsDouble() throws IOException {
//
//            return delegate().getValueAsDouble();
//        }
//
//        @Override
//        public double getValueAsDouble(final double defaultValue) throws IOException {
//
//            return delegate().getValueAsDouble(defaultValue);
//        }
//
//        @Override
//        public boolean getValueAsBoolean() throws IOException {
//
//            return delegate().getValueAsBoolean();
//        }
//
//        @Override
//        public boolean getValueAsBoolean(final boolean defaultValue) throws IOException {
//
//            return delegate().getValueAsBoolean(defaultValue);
//        }
//
//        @Override
//        public String getValueAsString() throws IOException {
//
//            return delegate().getValueAsString();
//        }
//
//        @Override
//        public String getValueAsString(final String defaultValue) throws IOException {
//
//            return delegate().getValueAsString(defaultValue);
//        }
//
//        @Override
//        public Object getEmbeddedObject() throws IOException {
//
//            return delegate().getEmbeddedObject();
//        }
//
//        @Override
//        public byte[] getBinaryValue(final Base64Variant b64variant) throws IOException {
//
//            return delegate().getBinaryValue(b64variant);
//        }
//
//        @Override
//        public int readBinaryValue(final Base64Variant b64variant, final OutputStream out) throws IOException {
//
//            return delegate().readBinaryValue(b64variant, out);
//        }
//
//        @Override
//        public JsonLocation getTokenLocation() {
//
//            return delegate().getTokenLocation();
//        }
//
//        @Override
//        public JsonToken nextValue() throws IOException {
//
//            return delegate().nextValue();
//        }
//
//        @Override
//        public void finishToken() throws IOException {
//
//            delegate().finishToken();
//        }
//
//        @Override
//        public JsonParser skipChildren() throws IOException {
//
//            delegate().skipChildren();
//            return this;
//        }
//
//        @Override
//        public boolean canReadObjectId() {
//
//            return delegate().canReadObjectId();
//        }
//
//        @Override
//        public boolean canReadTypeId() {
//
//            return delegate().canReadTypeId();
//        }
//
//        @Override
//        public Object getObjectId() throws IOException {
//
//            return delegate().getObjectId();
//        }
//
//        @Override
//        public Object getTypeId() throws IOException {
//
//            return delegate().getTypeId();
//        }
//
//        @SuppressWarnings("unused")
//        private interface Excludes {
//
//            JsonToken nextToken();
//
//            JsonParser enable(Feature f);
//
//            JsonParser disable(Feature f);
//
//            JsonParser setFeatureMask(int mask);
//
//            JsonParser overrideStdFeatures(int values, int mask);
//
//            JsonParser overrideFormatFeatures(int values, int mask);
//
//            JsonParser skipChildren() throws IOException;
//        }
    }*/

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

        boolean isEnabled(JsonFactory.Feature f);

        JsonFactory configure(com.fasterxml.jackson.core.JsonParser.Feature f, boolean state);

        boolean isEnabled(com.fasterxml.jackson.core.JsonParser.Feature f);

        JsonFactory configure(com.fasterxml.jackson.core.JsonGenerator.Feature f, boolean state);

        boolean isEnabled(com.fasterxml.jackson.core.JsonGenerator.Feature f);
    }
}
