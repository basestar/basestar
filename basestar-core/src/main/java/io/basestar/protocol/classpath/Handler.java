package io.basestar.protocol.classpath;

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

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

@RequiredArgsConstructor
@SuppressWarnings("unused")
public class Handler extends URLStreamHandler {

    private final ClassLoader classLoader;

    public Handler() {

        this(Handler.class.getClassLoader());
    }

    @Override
    protected URLConnection openConnection(final URL url) throws IOException {

        final String path;
        if(url.getPath().startsWith("/")) {
            path = url.getPath().substring(1);
        } else {
            path = url.getPath();
        }
        final URL resourceUrl = classLoader.getResource(path);
        if(resourceUrl != null) {
            return resourceUrl.openConnection();
        } else {
            throw new IllegalStateException(url.getPath() + " was not found on the classpath");
        }
    }
}
