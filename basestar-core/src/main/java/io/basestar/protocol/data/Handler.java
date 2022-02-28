package io.basestar.protocol.data;

/*-
 * #%L
 * basestar-core
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

import lombok.RequiredArgsConstructor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.util.Base64;

@RequiredArgsConstructor
@SuppressWarnings("unused")
public class Handler extends URLStreamHandler {

    private final ClassLoader classLoader;

    public Handler() {

        this(Handler.class.getClassLoader());
    }

    @Override
    protected URLConnection openConnection(final URL url) throws IOException {

        return new Connection(url);
    }

    public static class Connection extends URLConnection {

        private static final String DEFAULT_CONTENT_TYPE = "text/plain";

        private final String contentType;

        private final String encoding;

        private final String body;

        public String getContentType() {

            return guessContentTypeFromName(url.getFile());
        }

        public Connection(final URL url) throws IOException {

            super(url);
            final String path = url.getPath();
            final String[] parts = path.split(",", 2);
            if (parts[0].isEmpty()) {
                this.contentType = DEFAULT_CONTENT_TYPE;
                this.encoding = null;
            } else {
                final String[] types = parts[0].split(";");
                if (types[0].isEmpty()) {
                    this.contentType = DEFAULT_CONTENT_TYPE;
                } else {
                    this.contentType = types[0].trim();
                }
                this.encoding = types[1].trim();
            }
            this.body = parts[1].trim();
        }

        public void connect() throws IOException {


        }

        public InputStream getInputStream() throws IOException {

            if (!"BASE64".equalsIgnoreCase(encoding)) {
                throw new UnsupportedEncodingException("Only base64 encoding supported for data url");
            }
            final byte[] bytes = Base64.getDecoder().decode(body);
            return new ByteArrayInputStream(bytes);
        }
    }
}
