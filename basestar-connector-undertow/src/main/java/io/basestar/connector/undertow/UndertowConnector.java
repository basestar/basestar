package io.basestar.connector.undertow;

/*-
 * #%L
 * basestar-connector-undertow
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

import io.basestar.api.API;
import io.undertow.Undertow;
import lombok.Data;

@Data
public class UndertowConnector {

    private final Undertow server;

    @lombok.Builder(builderClassName = "Builder")
    UndertowConnector(final API api, final String host, final int port,
                      final Integer ioThreads, final Integer workerThreads,
                      final Integer bufferSize, final Boolean directBuffers) {

        final Undertow.Builder builder = Undertow.builder()
                .addHttpListener(port, host)
                .setHandler(new UndertowHandler(api));
        if(ioThreads != null) {
            builder.setIoThreads(ioThreads);
        }
        if(workerThreads != null) {
            builder.setWorkerThreads(workerThreads);
        }
        if(bufferSize != null) {
            builder.setBufferSize(bufferSize);
        }
        if(directBuffers != null) {
            builder.setDirectBuffers(directBuffers);
        }
        this.server = builder.build();
    }

    public void start() {

        server.start();
    }

    public void stop() {

        server.stop();
    }

}
