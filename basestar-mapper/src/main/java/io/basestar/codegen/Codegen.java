package io.basestar.codegen;

import freemarker.template.Version;
import freemarker.template.*;
import io.basestar.codegen.model.EnumSchemaModel;
import io.basestar.codegen.model.ObjectSchemaModel;
import io.basestar.codegen.model.StructSchemaModel;
import io.basestar.codegen.model.ViewSchemaModel;
import io.basestar.schema.*;

import java.io.IOException;
import java.io.Writer;
import java.util.Locale;

public class Codegen {

    private final Configuration cfg;

    private final CodegenSettings settings;

    public Codegen(final String language, final CodegenSettings settings) {

        final Configuration cfg = new Configuration(Configuration.VERSION_2_3_30);

        cfg.setClassForTemplateLoading(Codegen.class, "language/" + language);
        cfg.setIncompatibleImprovements(new Version(2, 3, 20));
        cfg.setDefaultEncoding("UTF-8");
        cfg.setLocale(Locale.US);
        cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);

        this.cfg = cfg;
        this.settings = settings;
    }

    public void generate(final Schema<?> schema, final Writer writer) throws IOException {

        try {
            if(schema instanceof EnumSchema) {
                final Template template = cfg.getTemplate("EnumSchema.java.ftl");
                template.process(new EnumSchemaModel(settings, (EnumSchema) schema), writer);
            } else if(schema instanceof ObjectSchema) {
                final Template template = cfg.getTemplate("ObjectSchema.java.ftl");
                template.process(new ObjectSchemaModel(settings, (ObjectSchema) schema), writer);
            } else if(schema instanceof StructSchema) {
                final Template template = cfg.getTemplate("StructSchema.java.ftl");
                template.process(new StructSchemaModel(settings, (StructSchema) schema), writer);
            } else if(schema instanceof ViewSchema) {
                final Template template = cfg.getTemplate("ViewSchema.java.ftl");
                template.process(new ViewSchemaModel(settings, (ViewSchema) schema), writer);
            } else {
                throw new IllegalStateException("Cannot process schema " + schema.getClass());
            }
        } catch (final TemplateException e) {
            throw new IOException(e);
        }
    }
}
