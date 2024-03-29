package io.basestar.api;

/*-
 * #%L
 * basestar-api
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

import io.basestar.util.CompletableFutures;
import io.swagger.v3.oas.models.OpenAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public interface API {

    CompletableFuture<APIResponse> handle(APIRequest request) throws IOException;

    default CompletableFuture<APIResponse> handleUnchecked(final APIRequest request) {

        final Logger log = LoggerFactory.getLogger(getClass());
        try {
            return handle(request);
        } catch (final IOException e) {
            log.error("Unhandled error", e);
            return CompletableFuture.completedFuture(APIResponse.error(request, 400, e));
        } catch (final Exception e) {
            log.error("Unhandled error", e);
            return CompletableFutures.completedExceptionally(e);
        }
    }

    CompletableFuture<OpenAPI> openApi();
}
