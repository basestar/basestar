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
import io.basestar.util.Pager;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface Subscriptions {

    CompletableFuture<?> subscribe(Caller caller, String sub, String channel, Set<Subscription.Key> keys, Expression expression, SubscriptionInfo info);

    List<Pager.Source<Subscription>> query(Set<Subscription.Key> keys);

    CompletableFuture<?> unsubscribe(String sub, String channel);

    CompletableFuture<?> unsubscribeAll(String sub);
}
