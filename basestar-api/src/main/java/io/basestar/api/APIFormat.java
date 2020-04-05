package io.basestar.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

@Getter
@RequiredArgsConstructor
public enum APIFormat {

    JSON("application/json", new ObjectMapper()),
    YAML("application/yaml", new YAMLMapper());

    //,
    //XML("application/xml", new ObjectMapper());

    private final String contentType;

    private final ObjectMapper mapper;

    public static Set<String> contentTypes() {

        return Arrays.stream(values()).map(APIFormat::getContentType)
                .collect(Collectors.toSet());
    }

    public static APIFormat forContentType(final String contentType) {

        return Arrays.stream(values()).filter(v -> v.getContentType().equals(contentType))
                .findFirst().orElse(null);
    }

    public static APIFormat bestMatch(final String contentType) {

        return bestMatch(contentType, null);
    }

    public static APIFormat bestMatch(final String contentType, final APIFormat defaultValue) {

        return APIFormat.JSON;
//        final String bestMatch = MIMEParse.bestMatch(APIFormat.contentTypes(), contentType);
//        return APIFormat.forContentType(bestMatch);
    }

    public static APIFormat forFormat(final String format) {

        return valueOf(format.toUpperCase());
    }
}
