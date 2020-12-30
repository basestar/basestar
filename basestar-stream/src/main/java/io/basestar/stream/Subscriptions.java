package io.basestar.stream;

/*-
 * #%L
 * basestar-stream
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

import io.basestar.auth.Caller;
import io.basestar.expression.Expression;
import io.basestar.schema.LinkableSchema;
import io.basestar.util.Pager;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface Subscriptions {

    CompletableFuture<?> subscribe(Caller caller, String sub, String channel, LinkableSchema schema, Set<Change.Event> events, Expression expression, SubscriptionMetadata info);

    Pager<Subscription> query(LinkableSchema schema, Change.Event event, Map<String, Object> before, Map<String, Object> after);

    CompletableFuture<?> unsubscribe(String sub, String channel);

    CompletableFuture<?> unsubscribeAll(String sub);
}
