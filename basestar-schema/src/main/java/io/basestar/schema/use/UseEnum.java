package io.basestar.schema.use;

/*-
 * #%L
 * basestar-schema
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

import io.basestar.schema.EnumSchema;
import lombok.Data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Enum Type
 *
 * <strong>Example</strong>
 * <pre>
 * type: MyEnum
 * </pre>
 */

@Data
public class UseEnum implements UseScalar<String> {

    private final EnumSchema type;

    @Override
    public <R> R visit(final Visitor<R> visitor) {

        return visitor.visitEnum(this);
    }

    public static UseEnum from(final EnumSchema type, final Object config) {

        return new UseEnum(type);
    }

    @Override
    public Object toJson() {

        return type.getName();
    }

    @Override
    @SuppressWarnings("unchecked")
    public String create(final Object value, final boolean expand) {

        return type.create(value, expand);
    }

    @Override
    public Code code() {

        return Code.ENUM;
    }

    @Override
    public void serializeValue(final String value, final DataOutput out) throws IOException {

        UseString.DEFAULT.serializeValue(value, out);
    }

    @Override
    public String deserializeValue(final DataInput in) throws IOException {

        return deserializeAnyValue(in);
    }

    public static String deserializeAnyValue(final DataInput in) throws IOException {

        return UseString.DEFAULT.deserializeValue(in);
    }

    @Override
    public String toString() {

        return type.getName();
    }

//    @Override
//    public Map<String, Object> openApiType() {
//
//        return type.openApiRef();
//    }
}
