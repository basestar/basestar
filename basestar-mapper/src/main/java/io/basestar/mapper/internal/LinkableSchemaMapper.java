package io.basestar.mapper.internal;

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

import io.basestar.schema.Bucketing;
import io.basestar.schema.LinkableSchema;
import io.basestar.util.Immutable;

import java.util.List;

public abstract class LinkableSchemaMapper<B extends LinkableSchema.Builder<B, ?>, T> extends InstanceSchemaMapper<B, T> {

    protected final List<Bucketing> bucketing;

    @SuppressWarnings("unchecked")
    protected LinkableSchemaMapper(final Class<B> builderType, final Builder<B, T> builder) {

        super(builderType, builder);
        this.bucketing = Immutable.list(builder.getBucketing());
    }

    protected B addMembers(final B builder) {

        members.forEach(m -> m.addToSchema(this, builder));
        builder.setBucket(bucketing);
        return builder;
    }

    public interface Builder<B extends LinkableSchema.Builder<B, ?>, T> extends InstanceSchemaMapper.Builder<B, T> {

        List<Bucketing> getBucketing();

        Builder<B, T> setBucketing(List<Bucketing> bucketing);

        LinkableSchemaMapper<B, T> build();
    }
}
