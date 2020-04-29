package io.basestar.protocol.classpath;

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
