package io.basestar.api;

import com.google.common.collect.Multimap;

import java.io.IOException;
import java.io.OutputStream;

public interface APIResponse {

    int getStatusCode();

    Multimap<String, String> getHeaders();

    void writeTo(OutputStream out) throws IOException;
}
