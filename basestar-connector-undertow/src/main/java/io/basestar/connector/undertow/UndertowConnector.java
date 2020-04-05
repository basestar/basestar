package io.basestar.connector.undertow;

import io.basestar.api.API;
import io.undertow.Undertow;
import lombok.Data;

@Data
public class UndertowConnector {

    private final Undertow server;

    public UndertowConnector(final API api, final String host, final int port) {

        this.server = Undertow.builder()
                .addHttpListener(port, host)
                .setHandler(new UndertowHandler(api))
                .build();
    }

    public void start() {

        server.start();
    }

    public void stop() {

        server.stop();
    }

}
