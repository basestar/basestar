package io.basestar.protocol.s3;

import com.google.common.collect.ImmutableList;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@Slf4j
@RequiredArgsConstructor
@SuppressWarnings("unused")
public class Handler extends URLStreamHandler {

    private final S3Client client;

    public Handler() {

        this(S3Client.create());
    }

    @Override
    protected URLConnection openConnection(final URL url) {

        return new URLConnection(url) {

            private ResponseInputStream<GetObjectResponse> inputStream;

            private Map<String, List<String>> headers;

            public String getHeaderField(final String name) {

                return getHeaderFields().getOrDefault(name, Collections.emptyList()).stream().findFirst().orElse(null);
            }

            public Map<String, List<String>> getHeaderFields() {

                connect();
                return Collections.unmodifiableMap(headers);
            }

            @Override
            public InputStream getInputStream() {

                connect();
                return inputStream;
            }

            @Override
            public void connect() {

                if(inputStream == null) {
                    try {
                        final String bucket = stripLeadingSlashes(url.getHost());
                        final String key = stripLeadingSlashes(url.getPath());

                        this.inputStream = client.getObject(GetObjectRequest.builder()
                                .bucket(bucket).key(key).build());

                        final GetObjectResponse response = inputStream.response();

                        this.headers = new HashMap<>();
                        headers.put("content-length", ImmutableList.of(Long.toString(response.contentLength())));
                        headers.put("content-type", ImmutableList.of(response.contentType()));
                        headers.put("content-encoding", ImmutableList.of(response.contentEncoding()));
                    } catch (final Exception e) {
                        log.error("Failed to connect to {}", url);
                        throw e;
                    }
                }
            }
        };
    }

    private static final Pattern LEADING_SLASHES_PATTERN = Pattern.compile("^/+");

    private static String stripLeadingSlashes(final String str) {

        return LEADING_SLASHES_PATTERN.matcher(str).replaceAll("");
    }
}
