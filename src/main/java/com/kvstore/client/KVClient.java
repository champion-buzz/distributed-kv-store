package com.kvstore.client;

import com.google.common.util.concurrent.ListenableFuture;
import com.kvstore.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Cluster-aware client.
 *
 * Reads are load balanced across all replicas with one of three strategies.
 * Writes go straight to the leader (discovered via ClusterInfo and cached), with
 * automatic retry and leader re-discovery on failover.
 */
public class KVClient implements AutoCloseable {

    public enum Strategy {
        /** Every request goes to the first node (baseline, no balancing). */
        SINGLE_NODE,
        /** Rotate through nodes. */
        ROUND_ROBIN,
        /** Pick the node with the fewest in-flight requests (power of two choices). */
        LEAST_OUTSTANDING
    }

    private final List<ManagedChannel> channels = new ArrayList<>();
    private final List<KeyValueGrpc.KeyValueBlockingStub> stubs = new ArrayList<>();
    private final List<KeyValueGrpc.KeyValueFutureStub> futureStubs = new ArrayList<>();
    private final AtomicInteger[] inFlight;
    private final AtomicInteger rr = new AtomicInteger();
    private final Strategy strategy;
    private volatile int leaderIdx = -1;
    private final Map<Integer, Integer> nodeIdToIdx = new java.util.concurrent.ConcurrentHashMap<>();

    public KVClient(List<String> addresses, Strategy strategy) {
        this.strategy = strategy;
        for (String addr : addresses) {
            ManagedChannel ch = ManagedChannelBuilder.forTarget(addr).usePlaintext().build();
            channels.add(ch);
            stubs.add(KeyValueGrpc.newBlockingStub(ch));
            futureStubs.add(KeyValueGrpc.newFutureStub(ch));
        }
        inFlight = new AtomicInteger[addresses.size()];
        for (int i = 0; i < inFlight.length; i++) inFlight[i] = new AtomicInteger();
    }

    private int pick() {
        int n = stubs.size();
        switch (strategy) {
            case SINGLE_NODE:
                return 0;
            case ROUND_ROBIN:
                return Math.floorMod(rr.getAndIncrement(), n);
            default: {
                java.util.concurrent.ThreadLocalRandom r = java.util.concurrent.ThreadLocalRandom.current();
                int a = r.nextInt(n), b = r.nextInt(n);
                return inFlight[a].get() <= inFlight[b].get() ? a : b;
            }
        }
    }

    private KeyValueGrpc.KeyValueBlockingStub stub(int i) {
        return stubs.get(i).withDeadlineAfter(5, TimeUnit.SECONDS);
    }

    // ------------------------------------------------------------------ reads

    public Optional<String> get(String key) {
        int start = pick();
        for (int attempt = 0; attempt < stubs.size(); attempt++) {
            int i = (start + attempt) % stubs.size();
            inFlight[i].incrementAndGet();
            try {
                GetResponse r = stub(i).get(GetRequest.newBuilder().setKey(key).build());
                return r.getFound() ? Optional.of(r.getValue()) : Optional.empty();
            } catch (Exception e) {
                // node down: fall through to the next replica
            } finally {
                inFlight[i].decrementAndGet();
            }
        }
        throw new IllegalStateException("no replica reachable");
    }

    /** Fetches many keys in parallel, spreading them over the replicas. */
    public Map<String, String> multiGet(Collection<String> keys) throws Exception {
        Map<String, ListenableFuture<GetResponse>> futures = new LinkedHashMap<>();
        for (String k : keys) {
            int i = pick();
            futures.put(k, futureStubs.get(i).withDeadlineAfter(5, TimeUnit.SECONDS)
                    .get(GetRequest.newBuilder().setKey(k).build()));
        }
        Map<String, String> out = new LinkedHashMap<>();
        for (Map.Entry<String, ListenableFuture<GetResponse>> e : futures.entrySet()) {
            GetResponse r = e.getValue().get();
            if (r.getFound()) out.put(e.getKey(), r.getValue());
        }
        return out;
    }

    // ------------------------------------------------------------------ writes

    public boolean put(String key, String value) {
        return write(s -> s.put(PutRequest.newBuilder().setKey(key).setValue(value).build()));
    }

    public boolean delete(String key) {
        return write(s -> s.delete(DeleteRequest.newBuilder().setKey(key).build()));
    }

    private boolean write(java.util.function.Function<KeyValueGrpc.KeyValueBlockingStub, WriteResponse> op) {
        for (int attempt = 0; attempt < 10; attempt++) {
            int i = leaderIdx >= 0 ? leaderIdx : Math.floorMod(rr.getAndIncrement(), stubs.size());
            try {
                WriteResponse r = op.apply(stub(i));
                if (r.getSuccess()) {
                    learnLeader(r.getLeaderId(), i);
                    return true;
                }
                leaderIdx = -1;
            } catch (Exception e) {
                leaderIdx = -1;
            }
            sleep(150L * (attempt + 1)); // back off while a new leader is elected
        }
        return false;
    }

    private void learnLeader(int leaderNodeId, int answeredBy) {
        if (nodeIdToIdx.isEmpty()) refreshTopology();
        Integer idx = nodeIdToIdx.get(leaderNodeId);
        leaderIdx = idx != null ? idx : answeredBy;
    }

    private synchronized void refreshTopology() {
        for (int i = 0; i < stubs.size(); i++) {
            try {
                ClusterInfoResponse info = stubs.get(i).withDeadlineAfter(1, TimeUnit.SECONDS)
                        .clusterInfo(Empty.getDefaultInstance());
                nodeIdToIdx.put(info.getNodeId(), i);
            } catch (Exception ignored) {
            }
        }
    }

    public List<ClusterInfoResponse> clusterInfo() {
        List<ClusterInfoResponse> out = new ArrayList<>();
        for (KeyValueGrpc.KeyValueBlockingStub s : stubs) {
            try {
                out.add(s.withDeadlineAfter(1, TimeUnit.SECONDS).clusterInfo(Empty.getDefaultInstance()));
            } catch (Exception ignored) {
            }
        }
        return out;
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        channels.forEach(ManagedChannel::shutdownNow);
    }
}
