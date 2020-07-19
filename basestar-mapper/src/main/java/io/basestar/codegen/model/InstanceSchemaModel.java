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

import com.google.common.collect.ImmutableList;
import io.basestar.codegen.CodegenSettings;
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.Reserved;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public abstract class InstanceSchemaModel extends SchemaModel {

    private final InstanceSchema schema;

    public InstanceSchemaModel(final CodegenSettings settings, final InstanceSchema schema) {

        super(settings, schema);
        this.schema = schema;
    }

    public List<MemberModel> getMembers() {

        return Stream.concat(Stream.concat(
                schema.getExtend() != null ? Stream.<MemberModel>empty() : schema.metadataSchema().entrySet().stream()
                        .filter(entry -> !Reserved.SCHEMA.equals(entry.getKey()))
                        .map(entry -> new MetadataModel(getSettings(), entry.getKey(), entry.getValue())),
                schema.getDeclaredProperties().values().stream()
                        .map(v -> new PropertyModel(getSettings(), v))
        ), getAdditionalMembers().stream()).sorted(Comparator.comparing(MemberModel::getName)).collect(Collectors.toList());
    }

    protected List<MemberModel> getAdditionalMembers() {

        return ImmutableList.of();
    }

    public abstract InstanceSchemaModel getExtend();
}
