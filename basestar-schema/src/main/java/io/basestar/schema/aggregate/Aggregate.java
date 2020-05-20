package io.basestar.schema.aggregate;

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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "function")
@JsonSubTypes({
        @JsonSubTypes.Type(name = Sum.TYPE, value = Sum.class),
        @JsonSubTypes.Type(name = Min.TYPE, value = Min.class),
        @JsonSubTypes.Type(name = Max.TYPE, value = Max.class),
        @JsonSubTypes.Type(name = Avg.TYPE, value = Avg.class),
        @JsonSubTypes.Type(name = Count.TYPE, value = Count.class)
})
public interface Aggregate {

    <T> T visit(AggregateVisitor<T> visitor);

//    Aggregate withOutput(Expression expression);
//
//    Expression getOutput();
}
