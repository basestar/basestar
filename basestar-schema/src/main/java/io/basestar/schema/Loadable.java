package io.basestar.schema;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import io.basestar.jackson.BasestarFactory;
import io.basestar.jackson.BasestarModule;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.Writer;

public interface Loadable extends Serializable {

    ObjectMapper JSON_MAPPER = new ObjectMapper(new BasestarFactory())
            .registerModule(BasestarModule.INSTANCE)
            // Do not use, Spark compatibility issue
            //.setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL)
            .configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);

    ObjectMapper YAML_MAPPER = new ObjectMapper(new BasestarFactory(new YAMLFactory()
            .configure(YAMLGenerator.Feature.USE_NATIVE_TYPE_ID, false)
            .configure(YAMLGenerator.Feature.WRITE_DOC_START_MARKER, false)
            .configure(YAMLGenerator.Feature.SPLIT_LINES, false)))
            .registerModule(BasestarModule.INSTANCE)
            .configure(JsonParser.Feature.ALLOW_COMMENTS, true)
            // Do not use, Spark compatibility issue
            //.setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL)
            .configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);

    interface Descriptor {

        @JsonValue
        Object jsonValue();

        default void yaml(final OutputStream os) throws IOException {

            YAML_MAPPER.writeValue(os, jsonValue());
        }

        default void yaml(final Writer os) throws IOException {

            YAML_MAPPER.writeValue(os, jsonValue());
        }

        default void json(final OutputStream os) throws IOException {

            JSON_MAPPER.writeValue(os, jsonValue());
        }

        default void json(final Writer os) throws IOException {

            JSON_MAPPER.writeValue(os, jsonValue());
        }
    }
}
