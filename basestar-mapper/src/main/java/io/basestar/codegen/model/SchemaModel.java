package io.basestar.codegen.model;

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

import com.google.common.collect.ImmutableSet;
import io.basestar.codegen.CodegenContext;
import io.basestar.schema.EnumSchema;
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.Schema;
import io.basestar.util.Name;
import io.basestar.util.Path;
import io.basestar.util.Text;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SuppressWarnings("unused")
public abstract class SchemaModel extends Model {

    private final Schema<?> schema;

    public SchemaModel(final CodegenContext context, final Schema<?> schema) {

        super(context);
        this.schema = schema;
    }

    public Name getQualifiedName() {

        return schema.getQualifiedName();
    }

    public String getClassName() {

        return Text.upperCamel(getQualifiedName().last());
    }

    public String getFullyQualifiedClassName() {

        final Name rootPackage = getContext().getRootPackage();
        final Name qualifiedName = getQualifiedName();
        return rootPackage.with(qualifiedName).toString();
    }

    public String getQualifiedClassName() {

        return getQualifiedName().toString();
    }

    public String getRelativeClassFile() {

        final Name qualifiedName = getQualifiedName();
        return Path.of(getContext().getRelativePackage()).relative(Path.of(qualifiedName)).toString(File.separatorChar);
    }

    public String getName() {

        return schema.getName();
    }

    public String getDescription() {

        return schema.getDescription();
    }

    public String getPackageName() {

        final String base = super.getPackageName();
        return Name.parse(base).with(schema.getQualifiedPackageName()).toString();
    }

    public List<SchemaModel> getSchemaDependencies() {

        final Map<Name, Schema<?>> dependencies = schema.dependencies(ImmutableSet.of());
        return dependencies.entrySet().stream()
                .filter(e -> !e.getKey().equals(schema.getQualifiedName()))
                .sorted(Map.Entry.comparingByKey())
                .map(e -> from(getContext(), e.getValue()))
                .collect(Collectors.toList());
    }

    public static SchemaModel from(final CodegenContext context, final Schema<?> schema) {

        if(schema instanceof EnumSchema) {
            return new EnumSchemaModel(context, (EnumSchema) schema);
        } else if(schema instanceof InstanceSchema) {
            return InstanceSchemaModel.from(context, (InstanceSchema)schema);
        } else {
            throw new IllegalStateException("Cannot process schema " + schema.getClass());
        }
    }

    public abstract String getSchemaType();

    public abstract List<AnnotationModel<?>> getAnnotations();

    public boolean generate() {

        return true;
    }
}
