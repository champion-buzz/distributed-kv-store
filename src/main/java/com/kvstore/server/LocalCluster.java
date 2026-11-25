package com.kvstore.server;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Starts an N-node cluster inside one JVM — handy for demos, tests and benchmarks. */
public class LocalCluster implements AutoCloseable {

    private final List<Node> nodes = new ArrayList<>();
    private final List<String> addresses = new ArrayList<>();

    public LocalCluster(int size, int basePort, Path dataDir, int workersPerNode) throws Exception {
        for (int i = 1; i <= size; i++) addresses.add("localhost:" + (basePort + i - 1));
        for (int i = 1; i <= size; i++) {
            Map<Integer, String> peers = new HashMap<>();
            for (int j = 1; j <= size; j++) if (j != i) peers.put(j, addresses.get(j - 1));
            nodes.add(new Node(i, basePort + i - 1, peers, dataDir, workersPerNode).start());
        }
    }

    public List<Node> nodes() { return nodes; }
    public List<String> addresses() { return addresses; }

    public Node awaitLeader(long timeoutMs) throws InterruptedException {
        long end = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < end) {
            for (Node n : nodes) if (n.role() == Node.Role.LEADER) return n;
            Thread.sleep(50);
        }
        throw new IllegalStateException("no leader elected within " + timeoutMs + " ms");
    }

    @Override
    public void close() {
        nodes.forEach(Node::stop);
    }

    public static void main(String[] args) throws Exception {
        int size = args.length > 0 ? Integer.parseInt(args[0]) : 3;
        LocalCluster c = new LocalCluster(size, 50051, Path.of("./data"), 8);
        Node leader = c.awaitLeader(5000);
        System.out.println("Cluster up: " + c.addresses() + "  leader = node " + leader.id());
        Runtime.getRuntime().addShutdownHook(new Thread(c::close));
        Thread.currentThread().join();
    }
}
