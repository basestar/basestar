package io.basestar.codegen;

import lombok.Data;

import java.util.Map;

@Data
public class LanguageConfig {

    private Map<String, Qualifier> qualifiers;

    @Data
    public static class Qualifier {

        private String output;
    }
}
