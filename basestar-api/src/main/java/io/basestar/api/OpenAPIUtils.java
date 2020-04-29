package io.basestar.api;

import com.google.common.collect.*;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.ExternalDocumentation;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.info.Info;

import java.util.*;

public class OpenAPIUtils {

    public static OpenAPI merge(final OpenAPI ... input) {

        return merge(Arrays.asList(input));
    }

    public static OpenAPI merge(final Collection<? extends OpenAPI> input) {

        final Multimap<String, OpenAPI> map = HashMultimap.create();
        input.forEach(v -> map.put("", v));
        return merge(map);
    }

    public static OpenAPI merge(final Map<String, OpenAPI> input) {

        final Multimap<String, OpenAPI> map = HashMultimap.create();
        input.forEach(map::put);
        return merge(map);
    }

    public static OpenAPI merge(final Multimap<String, OpenAPI> input) {

        final OpenAPI result = new OpenAPI();
        result.setInfo(mergeInfo(Multimaps.transformValues(input, OpenAPI::getInfo)));
        result.setTags(mergeLists(Multimaps.transformValues(input, OpenAPI::getTags)));
        result.setExtensions(mergeMaps(Multimaps.transformValues(input, OpenAPI::getExtensions)));
        result.setComponents(mergeComponents(Multimaps.transformValues(input, OpenAPI::getComponents)));
        result.setPaths(mergePaths(Multimaps.transformValues(input, OpenAPI::getPaths)));
        result.setSecurity(mergeLists(Multimaps.transformValues(input, OpenAPI::getSecurity)));
        result.setExternalDocs(mergeExternalDocs(Multimaps.transformValues(input, OpenAPI::getExternalDocs)));
        result.setServers(mergeLists(Multimaps.transformValues(input, OpenAPI::getServers)));
        return null;
    }

    private static Info mergeInfo(final Multimap<String, Info> input) {

        return input.values().stream().filter(Objects::nonNull).findFirst().orElse(null);
    }

    private static Components mergeComponents(final Multimap<String, Components> input) {

        final Components components = new Components();
        input.values().forEach(v -> {
            if (v != null) {
                v.getSchemas().forEach(components::addSchemas);
                v.getResponses().forEach(components::addResponses);
                v.getParameters().forEach(components::addParameters);
                v.getExamples().forEach(components::addExamples);
                v.getRequestBodies().forEach(components::addRequestBodies);
                v.getHeaders().forEach(components::addHeaders);
                v.getSecuritySchemes().forEach(components::addSecuritySchemes);
                v.getLinks().forEach(components::addLinks);
                v.getCallbacks().forEach(components::addCallbacks);
                v.getExtensions().forEach(components::addExtension);
            }
        });
        return components;
    }

    private static Paths mergePaths(final Multimap<String, Paths> input) {

        final Paths paths = new Paths();
        input.forEach((prefix, v) -> {
            if (v != null) {
                v.forEach((path, item) -> paths.put(prefix + path, item));
                v.getExtensions().forEach(paths::addExtension);
            }
        });
        return paths;
    }

    private static ExternalDocumentation mergeExternalDocs(final Multimap<String, ExternalDocumentation> input) {

        return input.values().stream().filter(Objects::nonNull).findFirst().orElse(null);
    }

    private static <V> List<V> mergeLists(final Multimap<String, List<V>> input) {

        return input.values().stream()
                .filter(Objects::nonNull)
                .reduce((a, b) -> ImmutableList.<V>builder().addAll(a).addAll(b).build())
                .orElse(null);
    }

    private static <K, V> Map<K, V> mergeMaps(final Multimap<String, Map<K, V>> input) {

        return input.values().stream()
                .filter(Objects::nonNull)
                .reduce((a, b) -> ImmutableMap.<K, V>builder().putAll(a).putAll(b).build())
                .orElse(null);
    }
}