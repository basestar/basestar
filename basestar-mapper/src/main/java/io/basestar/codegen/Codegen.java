package io.basestar.codegen;

/*-
 * #%L
 * basestar-mapper
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import freemarker.template.*;
import io.basestar.codegen.model.SchemaModel;
import io.basestar.jackson.BasestarModule;
import io.basestar.schema.Namespace;
import io.basestar.schema.Schema;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import io.basestar.util.Path;
import io.basestar.util.Text;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class Codegen {

    private static final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory()).registerModule(BasestarModule.INSTANCE);

    private final Configuration cfg;

    private final String language;

    private final LanguageConfig languageConfig;

    private final CodegenSettings settings;

    public Codegen(final String language, final CodegenSettings settings) {

        final Configuration cfg = new Configuration(Configuration.VERSION_2_3_30);

        cfg.setClassForTemplateLoading(Codegen.class, "language/" + language);
        cfg.setIncompatibleImprovements(new Version(2, 3, 20));
        cfg.setDefaultEncoding("UTF-8");
        cfg.setLocale(Locale.US);
        cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);

        this.cfg = cfg;
        this.language = language;
        try {
            this.languageConfig = objectMapper.readValue(getClass().getResource("language/" + language + "/config.yml"), LanguageConfig.class);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
        this.settings = settings;
    }

    private void generate(final SchemaModel model, final String qualifier, final Writer writer) throws IOException {

        try {
            final String schemaType = model.getSchemaType();
            final String templateName = Text.upperCamel(schemaType) + "Schema." + qualifier + ".ftl";
            final Template template = cfg.getTemplate(templateName);
            template.process(model, writer);
        } catch (final TemplateException e) {
            throw new IOException(e);
        }
    }

    public void generate(final Namespace namespace, final File base) throws IOException {

        generate(namespace, base, new Log() {

            @Override
            public void info(final String message) {

                log.info("{}", message);
            }

            @Override
            public void error(final String message, final Throwable error) {

                log.error("{}", message, error);
            }
        });
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void generate(final Namespace namespace, final File base, final Log log) throws IOException {

        for(final Map.Entry<String, LanguageConfig.Qualifier> entry : languageConfig.getQualifiers().entrySet()) {

            final String qualifierName = entry.getKey();
            final LanguageConfig.Qualifier qualifierConfig = entry.getValue();

            for (final Schema schema : namespace.getSchemas().values()) {

                final Name relativePackage = schema.getQualifiedPackageName();
                final CodegenContext context = CodegenContext.builder()
                        .rootPackage(Name.parse(settings.getPackageName()))
                        .relativePackage(relativePackage)
                        .codebehind(Nullsafe.orDefault(settings.getCodebehind(), Collections.emptyMap()))
                        .codebehindPath(Nullsafe.orDefault(settings.getCodebehindPath(), Path.of("codebehind")))
                        .build();
                final SchemaModel model = SchemaModel.from(context, schema);
                if (model.generate()) {
                    final String outputName = outputName(qualifierName, qualifierConfig, schema);
                    final File file = new File(base, outputName);
                    new File(file.getParent()).mkdirs();
                    try (final FileOutputStream fos = new FileOutputStream(file);
                         final OutputStreamWriter writer = new OutputStreamWriter(fos, Charsets.UTF_8)) {
                        log.info("Writing schema " + schema.getQualifiedName() + " to " + file.getAbsolutePath());
                        generate(model, qualifierName, writer);
                    }
                }
            }
        }
    }

    private String path(final Name name) {

        return (name == null || name.isEmpty()) ? "" : name.toString(File.separator) + File.separator;
    }

    private String outputName(final String qualifierName, final LanguageConfig.Qualifier qualifierConfig, final Schema schema) {

        final String pattern = qualifierConfig.getOutput();
        final Map<String, String> replacements = ImmutableMap.<String, String>builder()
                .put("qualifier", qualifierName)
                .put("settings.package.path", path(Name.parse(settings.getPackageName())))
                .put("schema.package.path", path(schema.getQualifiedPackageName()))
                .put("schema.name", schema.getName())
                .build();
        String result = pattern;
        for (final Map.Entry<String, String> entry : replacements.entrySet()) {
            result = result.replaceAll("\\{" + Pattern.quote(entry.getKey()) + "}", Matcher.quoteReplacement(entry.getValue()));
        }
        return result;
    }

    public interface Log {

        void info(String message);

        void error(String message, Throwable error);
    }
}
