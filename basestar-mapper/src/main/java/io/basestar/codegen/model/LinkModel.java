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

import io.basestar.codegen.CodegenSettings;
import io.basestar.schema.Link;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("unused")
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
    public List<AnnotationModel<?>> getAnnotations() {

        final List<AnnotationModel<?>> annotations = new ArrayList<>();
        annotations.add(new AnnotationModel<>(getSettings(), VALID));
        annotations.add(new AnnotationModel<>(getSettings(), io.basestar.mapper.annotation.Link.Declaration.annotation(link)));
        return annotations;
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
