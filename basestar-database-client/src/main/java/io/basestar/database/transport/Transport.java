package io.basestar.database.transport;

import com.google.common.collect.Multimap;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;

public interface Transport {

    <T> CompletableFuture<T> get(String url, Multimap<String, String> query,
                                 Multimap<String, String> headers, BodyReader<T> reader);

    <T> CompletableFuture<T> post(String url, Multimap<String, String> query,
                                  Multimap<String, String> headers, BodyWriter writer,
                                  BodyReader<T> reader);

    <T> CompletableFuture<T> put(String url, Multimap<String, String> query,
                                 Multimap<String, String> headers, BodyWriter writer,
                                 BodyReader<T> reader);

    <T> CompletableFuture<T> delete(String url, Multimap<String, String> query,
                                    Multimap<String, String> headers, BodyReader<T> reader);

    interface BodyReader<T> {

        T readFrom(InputStream is);
    }

    interface BodyWriter {

        void writeTo(OutputStream os);
    }
}
