//package io.basestar.storage.hazelcast;

/*-
 * #%L
 * basestar-storage-hazelcast
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
//
//import com.hazelcast.core.EntryEvent;
//import com.hazelcast.core.EntryListener;
//import com.hazelcast.map.MapEvent;
//import io.basestar.storage.BatchResponse;
//import io.basestar.storage.hazelcast.serde.CustomPortable;
//
//public class EventAdaptor implements EntryListener<BatchResponse.Key, CustomPortable> {
//
//    private final Emitter emitter;
//
//    @Override
//    public void entryAdded(final EntryEvent<BatchResponse.Key, CustomPortable> entryEvent) {
//
//    }
//
//    @Override
//    public void entryEvicted(final EntryEvent<BatchResponse.Key, CustomPortable> entryEvent) {
//
//    }
//
//    @Override
//    public void entryExpired(final EntryEvent<BatchResponse.Key, CustomPortable> entryEvent) {
//
//    }
//
//    @Override
//    public void entryRemoved(final EntryEvent<BatchResponse.Key, CustomPortable> entryEvent) {
//
//    }
//
//    @Override
//    public void entryUpdated(final EntryEvent<BatchResponse.Key, CustomPortable> entryEvent) {
//
//    }
//
//    @Override
//    public void mapCleared(final MapEvent mapEvent) {
//
//    }
//
//    @Override
//    public void mapEvicted(final MapEvent mapEvent) {
//
//    }
//}
