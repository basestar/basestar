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
import io.basestar.schema.Link;

import java.util.List;

public class LinkModel extends MemberModel {

    private final Link link;

    public LinkModel(final CodegenSettings settings, final Link link) {

        super(settings);

        this.link = link;
    }

    @Override
    public String getName() {

        return link.getName();
    }

    @Override
    public List<AnnotationModel> getAnnotations() {

        return ImmutableList.of(
                new AnnotationModel(getSettings(), io.basestar.mapper.annotation.Link.class, ImmutableMap.of(
                        "name", link.getName(),
                        "expression", link.getExpression()
                ))
        );
    }

    @Override
    public TypeModel getType() {

        return TypeModel.from(getSettings(), link.getType());
    }

    @Override
    public boolean isRequired() {

        return false;
    }
}
