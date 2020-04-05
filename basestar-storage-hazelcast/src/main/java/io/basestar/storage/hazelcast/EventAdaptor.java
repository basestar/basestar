//package io.basestar.storage.hazelcast;
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
