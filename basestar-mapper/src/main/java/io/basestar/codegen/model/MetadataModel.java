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
import com.google.common.collect.ImmutableMap;
import io.basestar.codegen.CodegenSettings;
import io.basestar.schema.Reserved;
import io.basestar.schema.use.Use;

import java.util.List;

@SuppressWarnings("unused")
public class MetadataModel extends MemberModel {

    private final String name;

    private final Use<?> type;

    public MetadataModel(final CodegenSettings settings, final String name, final Use<?> type) {

        super(settings);
        this.name = name;
        this.type = type;
    }

    @Override
    public String getName() {

        return name;
    }

    protected Class<?> getAnnotationClass() {

        switch (name) {
            case Reserved.ID:
                return io.basestar.mapper.annotation.Id.class;
            case Reserved.VERSION:
                return io.basestar.mapper.annotation.Version.class;
            case Reserved.CREATED:
                return io.basestar.mapper.annotation.Created.class;
            case Reserved.UPDATED:
                return io.basestar.mapper.annotation.Updated.class;
            case Reserved.HASH:
                return io.basestar.mapper.annotation.Hash.class;
            default:
                throw new UnsupportedOperationException("Invalid metadata " + name);
        }
    }


    @Override
    public List<AnnotationModel> getAnnotations() {

        return ImmutableList.of(
                new AnnotationModel(getSettings(), getAnnotationClass(), ImmutableMap.of())
        );
    }

    @Override
    public TypeModel getType() {

        return TypeModel.from(getSettings(), type);
    }

    @Override
    public boolean isRequired() {

        return false;
    }
}
