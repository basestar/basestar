package io.basestar.storage.hazelcast;

public class TestHazelcastCoordinator {

//    @Test
//    public void testLock() {
//
//        final Config config = new Config();
//        config.getCPSubsystemConfig().setCPMemberCount(3);
//
//        final HazelcastInstance a = Hazelcast.newHazelcastInstance(config);
//        Hazelcast.newHazelcastInstance(config);
//        Hazelcast.newHazelcastInstance(config);
//
//        final HazelcastCoordinator coordinator = HazelcastCoordinator.builder()
//                .instance(a).build();
//
//        try(final CloseableLock ignored = coordinator.lock(ImmutableSet.of("a", "b", "c"))) {
//            System.err.println(ignored);
//        }
//
//        try(final CloseableLock ignored = coordinator.lock(ImmutableSet.of("a", "b", "c"))) {
//            System.err.println(ignored);
//        }
//    }
}
