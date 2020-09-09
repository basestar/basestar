package io.basestar.codegen.model;

import com.google.common.collect.ImmutableList;
import io.basestar.codegen.Codebehind;
import io.basestar.codegen.CodegenContext;
import io.basestar.schema.InstanceSchema;
import io.basestar.util.Path;

import java.io.File;
import java.util.List;

public class CodebehindModel extends InstanceSchemaModel {

    private final Codebehind codebehind;

    public CodebehindModel(final CodegenContext context, final Codebehind codebehind, final InstanceSchema schema) {

        super(context, schema);
        this.codebehind = codebehind;
    }

//    @Override
//    public Name getQualifiedName() {
//
//        return name;
//    }

    @Override
    public String getRelativeClassFile() {

        final CodegenContext context = getContext();
        final Path codebehindPath = context.getCodebehindPath();
        final Path relative = Path.of(getContext().getRelativePackage());
        final Path classFile = Path.of(codebehind.getName());
        return Path.empty().up(relative.size()).with(codebehindPath).with(classFile).toString(File.separatorChar);
    }

//    @Override
//    public InstanceSchemaModel getExtend() {
//
//        if(codebehind.isExtend()) {
//            final InstanceSchema extend = schema.getExtend();
//            return extend == null ? null : InstanceSchemaModel.from(getContext(), extend);
//        } else {
//            return null;
//        }
//    }

    @Override
    public String getSchemaType() {

        return schema.descriptor().getType();
    }

    @Override
    public List<AnnotationModel<?>> getAnnotations() {

        return ImmutableList.of();
    }

    public boolean generate() {

        return codebehind.isGenerate();
    }
}
