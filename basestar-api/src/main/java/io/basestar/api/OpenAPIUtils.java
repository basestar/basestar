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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.basestar.util.Nullsafe;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.ExternalDocumentation;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.servers.Server;
import io.swagger.v3.oas.models.tags.Tag;

import java.util.*;
import java.util.stream.Stream;

public class OpenAPIUtils {

    private OpenAPIUtils() {

    }

    public static OpenAPI prefix(final OpenAPI input, final String prefix) {

        final OpenAPI result = new OpenAPI();
        result.setInfo(input.getInfo());
        result.setTags(input.getTags());
        result.setExtensions(input.getExtensions());
        result.setComponents(input.getComponents());
        result.setPaths(prefix(input.getPaths(), prefix));
        result.setSecurity(input.getSecurity());
        result.setExternalDocs(input.getExternalDocs());
        result.setServers(input.getServers());
        return result;
    }

    public static Paths prefix(final Paths input, final String prefix) {

        if (input == null) {
            return null;
        } else {
            final Paths paths = new Paths();
            input.forEach((k, v) -> {
                if (v != null) {
                    paths.put(joinPaths(prefix, k), v);
                }
            });
            paths.extensions(input.getExtensions());
            return paths;
        }
    }

    private static String joinPaths(final String a, final String b) {

        if (a.endsWith("/") && b.startsWith("/")) {
            return a + b.substring(1);
        } else {
            return a + b;
        }
    }

    public static OpenAPI merge(final OpenAPI... input) {

        return merge(Arrays.asList(input));
    }

    public static OpenAPI merge(final Collection<? extends OpenAPI> input) {

        final OpenAPI result = new OpenAPI();
        result.setInfo(mergeInfo(input));
        result.setTags(mergeTags(input));
        result.setExtensions(mergeExtensions(input));
        result.setComponents(mergeComponents(input));
        result.setPaths(mergePaths(input));
        result.setSecurity(mergeSecurity(input));
        result.setExternalDocs(mergeExternalDocs(input));
        result.setServers(mergeServers(input));
        return result;
    }

    private static Info mergeInfo(final Collection<? extends OpenAPI> input) {

        return input.stream().map(OpenAPI::getInfo).filter(Objects::nonNull).findFirst().orElse(null);
    }

    private static Components mergeComponents(final Collection<? extends OpenAPI> input) {

        final Components components = new Components();
        input.stream().map(OpenAPI::getComponents).forEach(v -> {
            if (v != null) {
                Nullsafe.orDefault(v.getSchemas()).forEach(components::addSchemas);
                Nullsafe.orDefault(v.getResponses()).forEach(components::addResponses);
                Nullsafe.orDefault(v.getParameters()).forEach(components::addParameters);
                Nullsafe.orDefault(v.getExamples()).forEach(components::addExamples);
                Nullsafe.orDefault(v.getRequestBodies()).forEach(components::addRequestBodies);
                Nullsafe.orDefault(v.getHeaders()).forEach(components::addHeaders);
                Nullsafe.orDefault(v.getSecuritySchemes()).forEach(components::addSecuritySchemes);
                Nullsafe.orDefault(v.getLinks()).forEach(components::addLinks);
                Nullsafe.orDefault(v.getCallbacks()).forEach(components::addCallbacks);
                Nullsafe.orDefault(v.getExtensions()).forEach(components::addExtension);
            }
        });
        return components;
    }

    private static Paths mergePaths(final Collection<? extends OpenAPI> input) {

        final Paths paths = new Paths();
        input.forEach(v -> {
            if (v.getPaths() != null) {
                v.getPaths().forEach(paths::put);
                Nullsafe.orDefault(v.getExtensions()).forEach(paths::addExtension);
            }
        });
        return paths;
    }

    private static ExternalDocumentation mergeExternalDocs(final Collection<? extends OpenAPI> input) {

        return input.stream().map(OpenAPI::getExternalDocs).filter(Objects::nonNull).findFirst().orElse(null);
    }

    private static List<Tag> mergeTags(final Collection<? extends OpenAPI> input) {

        return mergeLists(input.stream().map(OpenAPI::getTags));
    }

    private static List<SecurityRequirement> mergeSecurity(final Collection<? extends OpenAPI> input) {

        return mergeLists(input.stream().map(OpenAPI::getSecurity));
    }

    private static List<Server> mergeServers(final Collection<? extends OpenAPI> input) {

        return mergeLists(input.stream().map(OpenAPI::getServers));
    }

    private static <V> List<V> mergeLists(final Stream<List<V>> input) {

        return input.filter(Objects::nonNull)
                .reduce((a, b) -> ImmutableList.<V>builder().addAll(a).addAll(b).build())
                .orElse(null);
    }

    private static Map<String, Object> mergeExtensions(final Collection<? extends OpenAPI> input) {

        return input.stream()
                .map(OpenAPI::getExtensions)
                .filter(Objects::nonNull)
                .reduce((a, b) -> ImmutableMap.<String, Object>builder().putAll(a).putAll(b).build())
                .orElse(null);
    }
}
