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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.basestar.jackson.BasestarModule;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.commonjava.mimeparse.MIMEParse;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

@Getter
@RequiredArgsConstructor
public enum APIFormat {

    JSON("application/json", new ObjectMapper().registerModule(BasestarModule.INSTANCE)
            .setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL)),

    YAML("application/yaml", new YAMLMapper().registerModule(BasestarModule.INSTANCE)
            .setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL)),

    XML("application/xml", new XmlMapper().registerModule(BasestarModule.INSTANCE)
            .setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL));

    private final String mimeType;

    private final ObjectMapper mapper;

    public static Set<String> contentTypes() {

        return Arrays.stream(values()).map(APIFormat::getMimeType)
                .collect(Collectors.toSet());
    }

    public static APIFormat forMimeType(final String mimeType) {

        return Arrays.stream(values()).filter(v -> v.getMimeType().equals(mimeType))
                .findFirst().orElse(null);
    }

    public static APIFormat bestMatch(final String mimeType) {

        return bestMatch(mimeType, null);
    }

    public static APIFormat bestMatch(final String mimeType, final APIFormat defaultValue) {

        final String bestMatch = MIMEParse.bestMatch(APIFormat.contentTypes(), mimeType);
        return APIFormat.forMimeType(bestMatch);
    }

    public static APIFormat forFormat(final String format) {

        return valueOf(format.toUpperCase());
    }
}
