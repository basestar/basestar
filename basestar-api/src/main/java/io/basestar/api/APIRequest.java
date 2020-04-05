package io.basestar.api;

import com.google.common.collect.Multimap;
import io.basestar.api.exception.UnsupportedContentException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

public interface APIRequest {

    Method getMethod();

    String getPath();

    Multimap<String, String> getQuery();

    Multimap<String, String> getHeaders();

    InputStream readBody() throws IOException;

    default String getAuthorization() {

        return getFirstHeader("Authorization");
    }

    default String getFirstHeader(final String key) {

        final Collection<String> result = getHeaders().get(key);
        return result.isEmpty() ? null : result.iterator().next();
    }

    default String getFirstQuery(final String key) {

        final Collection<String> result = getQuery().get(key);
        return result.isEmpty() ? null : result.iterator().next();
    }

    default APIFormat getContentType() {

        final String format = getFirstQuery("format");
        final String contentType = getFirstHeader("Content-Type");
        if(format == null && contentType == null) {
            return APIFormat.JSON;
        } else if(format != null) {
            return APIFormat.forFormat(format);
        } else {
            final APIFormat match = APIFormat.bestMatch(contentType);
            if (match == null) {
                throw new UnsupportedContentException(contentType);
            }
            return match;
        }
    }

    default APIFormat getAccept() {

        final String format = getFirstQuery("format");
        final String accept = getFirstHeader("Accept");
        if(format == null && accept == null) {
            return getContentType();
        } else if(format != null) {
            return APIFormat.forFormat(format);
        } else {
            final APIFormat match = APIFormat.bestMatch(accept);
            if (match == null) {
                throw new UnsupportedContentException(accept);
            }
            return match;
        }
    }

    enum Method {

        HEAD,
        OPTIONS,
        GET,
        POST,
        PATCH,
        PUT,
        DELETE
    }
}
