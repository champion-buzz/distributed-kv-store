package com.kvstore;

import com.kvstore.client.KVClient;
import com.kvstore.server.LocalCluster;
import com.kvstore.server.Node;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class ClusterTest {

    @TempDir
    Path tmp;

    private static void eventually(java.util.function.BooleanSupplier cond, long ms) throws InterruptedException {
        long end = System.currentTimeMillis() + ms;
        while (System.currentTimeMillis() < end) {
            if (cond.getAsBoolean()) return;
            Thread.sleep(50);
        }
        fail("condition not met within " + ms + " ms");
    }

    @Test
    void electsExactlyOneLeaderAndReplicatesWrites() throws Exception {
        try (LocalCluster c = new LocalCluster(3, 52051, tmp, 4);
             KVClient client = new KVClient(c.addresses(), KVClient.Strategy.ROUND_ROBIN)) {
            c.awaitLeader(5000);
            Thread.sleep(300);
            long leaders = c.nodes().stream().filter(n -> n.role() == Node.Role.LEADER).count();
            assertEquals(1, leaders);

            assertTrue(client.put("city", "Tempe"));
            eventually(() -> c.nodes().stream().allMatch(n ->
                    n.store().get("city") != null && n.store().get("city").value().equals("Tempe")), 3000);

            assertTrue(client.delete("city"));
            eventually(() -> c.nodes().stream().allMatch(n -> n.store().get("city") == null), 3000);
        }
    }

    @Test
    void handlesConcurrentWritersConsistently() throws Exception {
        try (LocalCluster c = new LocalCluster(3, 52061, tmp, 8);
             KVClient client = new KVClient(c.addresses(), KVClient.Strategy.LEAST_OUTSTANDING)) {
            c.awaitLeader(5000);
            ExecutorService pool = Executors.newFixedThreadPool(16);
            List<Future<Boolean>> fs = new ArrayList<>();
            for (int i = 0; i < 400; i++) {
                int n = i;
                fs.add(pool.submit(() -> client.put("k" + n, "v" + n)));
            }
            for (Future<Boolean> f : fs) assertTrue(f.get());
            pool.shutdown();

            eventually(() -> c.nodes().stream().allMatch(n -> n.store().size() == 400), 5000);
            Map<String, String> got = client.multiGet(List.of("k0", "k199", "k399"));
            assertEquals(Map.of("k0", "v0", "k199", "v199", "k399", "v399"), got);
        }
    }

    @Test
    void survivesLeaderFailure() throws Exception {
        try (LocalCluster c = new LocalCluster(3, 52071, tmp, 4)) {
            Node oldLeader = c.awaitLeader(5000);
            try (KVClient client = new KVClient(c.addresses(), KVClient.Strategy.ROUND_ROBIN)) {
                assertTrue(client.put("before", "1"));
                eventually(() -> c.nodes().stream().allMatch(n -> n.store().get("before") != null), 3000);

                oldLeader.stop();

                // a new leader must be elected among the 2 survivors and writes keep working
                eventually(() -> c.nodes().stream().anyMatch(n -> n != oldLeader && n.role() == Node.Role.LEADER), 5000);
                assertTrue(client.put("after", "2"));
                assertEquals(Optional.of("1"), client.get("before"));
                assertEquals(Optional.of("2"), client.get("after"));
            }
        }
    }

    @Test
    void recoversStateFromWriteAheadLog() throws Exception {
        try (LocalCluster c = new LocalCluster(3, 52081, tmp, 4);
             KVClient client = new KVClient(c.addresses(), KVClient.Strategy.ROUND_ROBIN)) {
            c.awaitLeader(5000);
            for (int i = 0; i < 50; i++) assertTrue(client.put("p" + i, "x" + i));
            client.delete("p7");
            eventually(() -> c.nodes().stream().allMatch(n -> n.store().size() == 49), 3000);
        }
        // restart on the same data dir — state must come back from disk
        try (LocalCluster c2 = new LocalCluster(3, 52091, tmp, 4)) {
            for (Node n : c2.nodes()) {
                assertEquals(49, n.store().size());
                assertNull(n.store().get("p7"));
                assertEquals("x49", n.store().get("p49").value());
            }
        }
    }
}
