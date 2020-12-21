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

import io.basestar.codegen.CodegenContext;
import io.basestar.mapper.annotation.Description;
import io.basestar.schema.Link;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("unused")
public class LinkModel extends MemberModel {

    private final Link link;

    public LinkModel(final CodegenContext context, final Link link) {

        super(context);

        this.link = link;
    }

    @Override
    public String getName() {

        return link.getName();
    }

    @Override
    public List<AnnotationModel<?>> getAnnotations() {

        final List<AnnotationModel<?>> annotations = new ArrayList<>();
        annotations.add(new AnnotationModel<>(getContext(), VALID));
        annotations.add(new AnnotationModel<>(getContext(), io.basestar.mapper.annotation.Link.Declaration.annotation(link)));
        if(link.getDescription() != null) {
            annotations.add(new AnnotationModel<>(getContext(), Description.Modifier.annotation(link.getDescription())));
        }
        return annotations;
    }

    @Override
    public TypeModel getType() {

        return TypeModel.from(getContext(), link.typeOf());
    }

    @Override
    public boolean isRequired() {

        return false;
    }
}
