package io.basestar.util;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class URLs {

    private URLs() {

    }

    public static List<URL> all(final List<URL> urls) {

        return all(urls.stream()).collect(Collectors.toList());
    }

    public static URL[] all(final URL ... urls) {

        return all(Arrays.stream(urls)).toArray(URL[]::new);
    }

    public static Stream<URL> all(final Stream<URL> urls) {

        return urls.flatMap(url -> {
            if(isLocalFile(url)) {
                try {
                    final Path path = Path.parse(url.getPath());
                    return path.resolve().map(v -> toURLUnchecked(v.toFileUri()));
                } catch (final IOException e) {
                    throw new UncheckedIOException("Failed to read url " + url, e);
                }
            }
            return Stream.of(url);
        });
    }

    public static URL toURLUnchecked(final URI uri) {

        try {
            return uri.toURL();
        } catch (final MalformedURLException e) {
            throw new UnsupportedOperationException();
        }
    }

    public static URL toURLUnchecked(final String url) {

        try {
            return new URL(url);
        } catch (final MalformedURLException e) {
            throw new IllegalStateException(e);
        }
    }

    public static boolean isLocalFile(final URL url) {

        // Copied this check from jackson TokenStreamFactory, assume % is some network path indicator
        if(url != null && "file".equals(url.getProtocol())) {
            final String host = url.getHost();
            if (host == null || host.length() == 0) {
                final String path = url.getPath();
                return path.indexOf('%') < 0;
            }
        }
        return false;
    }
}
