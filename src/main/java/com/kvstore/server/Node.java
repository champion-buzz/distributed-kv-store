package com.kvstore.server;

import com.kvstore.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * One node of the cluster. Every node runs the same code; roles change at runtime.
 *
 * Coordination (a simplified Raft):
 *  - The leader sends heartbeats every {@link #HEARTBEAT_MS} ms.
 *  - A follower that hears nothing for a randomized election timeout becomes a
 *    candidate, bumps the term and asks peers for votes. A majority makes it leader.
 *  - Votes are only granted to candidates whose log is at least as up to date.
 *
 * Writes:
 *  - Go to the leader (followers transparently forward them).
 *  - The leader appends locally, then replicates in parallel to followers using one
 *    ordered single-thread executor per peer, and replies once a majority acked.
 *  - A follower that detects a gap pulls a full snapshot from the leader.
 *
 * Reads:
 *  - Any node serves reads from its local copy, so clients can spread read load
 *    across replicas (eventually consistent on followers).
 */
public class Node {

    private static final Logger LOG = Logger.getLogger(Node.class.getName());
    static final long HEARTBEAT_MS = 100;
    static final long ELECTION_MIN_MS = 400;
    static final long ELECTION_MAX_MS = 800;
    static final long REPLICATION_TIMEOUT_MS = 2000;

    public enum Role { FOLLOWER, CANDIDATE, LEADER }

    private final int id;
    private final int port;
    private final Map<Integer, String> peerAddrs;               // peerId -> host:port
    private final Map<Integer, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final Map<Integer, ExecutorService> peerQueues = new ConcurrentHashMap<>();
    private final KVStore store;
    private final int workerThreads;

    private volatile Role role = Role.FOLLOWER;
    private volatile long term = 0;
    private volatile int leaderId = -1;
    private volatile int votedFor = -1;
    private volatile long lastHeartbeat = System.currentTimeMillis();
    private volatile boolean syncing = false;
    private volatile long indexAtLastHeartbeat = -1;

    private final Object stateLock = new Object();
    private final ScheduledExecutorService timers = Executors.newScheduledThreadPool(2);
    private final ExecutorService rpcPool = Executors.newCachedThreadPool();
    private Server server;
    private volatile long electionTimeout = randomTimeout();

    public Node(int id, int port, Map<Integer, String> peers, Path dataDir, int workerThreads) throws IOException {
        this.id = id;
        this.port = port;
        this.peerAddrs = new TreeMap<>(peers);
        this.store = new KVStore(dataDir, id);
        this.workerThreads = workerThreads;
        for (Integer pid : peerAddrs.keySet()) {
            peerQueues.put(pid, Executors.newSingleThreadExecutor());
        }
    }

    // ------------------------------------------------------------------ lifecycle

    public Node start() throws IOException {
        // A bounded worker pool models a node's finite capacity; it is what makes
        // spreading load across replicas matter.
        server = ServerBuilder.forPort(port)
                .executor(Executors.newFixedThreadPool(workerThreads))
                .addService(new KeyValueService())
                .addService(new ReplicationService())
                .build()
                .start();
        timers.scheduleAtFixedRate(this::tick, HEARTBEAT_MS, HEARTBEAT_MS / 2, TimeUnit.MILLISECONDS);
        LOG.info(() -> "node " + id + " listening on " + port + " (lastIndex=" + store.lastIndex() + ")");
        return this;
    }

    public void stop() {
        timers.shutdownNow();
        rpcPool.shutdownNow();
        peerQueues.values().forEach(ExecutorService::shutdownNow);
        if (server != null) server.shutdownNow();
        channels.values().forEach(ManagedChannel::shutdownNow);
        store.close();
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) server.awaitTermination();
    }

    public int id() { return id; }
    public Role role() { return role; }
    public long term() { return term; }
    public int leaderId() { return leaderId; }
    public KVStore store() { return store; }

    private int clusterSize() { return peerAddrs.size() + 1; }
    private int majority() { return clusterSize() / 2 + 1; }

    private static long randomTimeout() {
        return ThreadLocalRandom.current().nextLong(ELECTION_MIN_MS, ELECTION_MAX_MS);
    }

    private ManagedChannel channel(int peerId) {
        return channels.computeIfAbsent(peerId, pid ->
                ManagedChannelBuilder.forTarget(peerAddrs.get(pid)).usePlaintext().build());
    }

    private ReplicationGrpc.ReplicationBlockingStub repl(int peerId, long timeoutMs) {
        return ReplicationGrpc.newBlockingStub(channel(peerId))
                .withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS);
    }

    // ------------------------------------------------------------------ timers

    private void tick() {
        try {
            if (role == Role.LEADER) {
                sendHeartbeats();
            } else if (System.currentTimeMillis() - lastHeartbeat > electionTimeout) {
                startElection();
            }
        } catch (Exception e) {
            LOG.warning("tick failed: " + e);
        }
    }

    private void sendHeartbeats() {
        HeartbeatRequest req = HeartbeatRequest.newBuilder()
                .setLeaderId(id).setTerm(term).setCommitIndex(store.lastIndex()).build();
        for (Integer pid : peerAddrs.keySet()) {
            rpcPool.submit(() -> {
                try {
                    HeartbeatResponse r = repl(pid, HEARTBEAT_MS * 2).heartbeat(req);
                    if (r.getTerm() > term) stepDown(r.getTerm(), -1);
                } catch (Exception ignored) {
                    // peer down — it will catch up via snapshot when it returns
                }
            });
        }
    }

    private void startElection() {
        long electionTerm;
        synchronized (stateLock) {
            role = Role.CANDIDATE;
            term++;
            electionTerm = term;
            votedFor = id;
            leaderId = -1;
            lastHeartbeat = System.currentTimeMillis();
            electionTimeout = randomTimeout();
        }
        LOG.info(() -> "node " + id + " starting election for term " + electionTerm);

        AtomicInteger votes = new AtomicInteger(1);
        VoteRequest req = VoteRequest.newBuilder()
                .setCandidateId(id).setTerm(electionTerm).setLastIndex(store.lastIndex()).build();
        CountDownLatch done = new CountDownLatch(peerAddrs.size());
        for (Integer pid : peerAddrs.keySet()) {
            rpcPool.submit(() -> {
                try {
                    VoteResponse r = repl(pid, 300).requestVote(req);
                    if (r.getGranted()) votes.incrementAndGet();
                    else if (r.getTerm() > electionTerm) stepDown(r.getTerm(), -1);
                } catch (Exception ignored) {
                } finally {
                    done.countDown();
                }
            });
        }
        try {
            done.await(400, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        synchronized (stateLock) {
            if (role == Role.CANDIDATE && term == electionTerm && votes.get() >= majority()) {
                role = Role.LEADER;
                leaderId = id;
                LOG.info(() -> "node " + id + " became LEADER for term " + electionTerm
                        + " with " + votes.get() + " votes");
            }
        }
        if (role == Role.LEADER) sendHeartbeats();
    }

    private void stepDown(long newTerm, int newLeader) {
        synchronized (stateLock) {
            if (newTerm > term) {
                term = newTerm;
                votedFor = -1;
            }
            role = Role.FOLLOWER;
            if (newLeader >= 0) leaderId = newLeader;
            lastHeartbeat = System.currentTimeMillis();
        }
    }

    // ------------------------------------------------------------------ writes

    private void handleWrite(OpType op, String key, String value, StreamObserver<WriteResponse> out) {
        if (role != Role.LEADER) {
            forwardToLeader(op, key, value, out);
            return;
        }
        out.onNext(write(op, key, value));
        out.onCompleted();
    }

    private WriteResponse write(OpType op, String key, String value) {

        LogEntry entry = store.appendAsLeader(term, op, key, value);
        ReplicateRequest req = ReplicateRequest.newBuilder()
                .setLeaderId(id).setTerm(term).setEntry(entry).build();

        // Fan out in parallel; per-peer single-thread queues keep entries in order.
        AtomicInteger acks = new AtomicInteger(1); // self
        CountDownLatch quorum = new CountDownLatch(majority() - 1);
        for (Integer pid : peerAddrs.keySet()) {
            peerQueues.get(pid).submit(() -> {
                try {
                    ReplicateResponse r = repl(pid, REPLICATION_TIMEOUT_MS).replicate(req);
                    if (r.getOk()) {
                        acks.incrementAndGet();
                        quorum.countDown();
                    } else if (r.getTerm() > term) {
                        stepDown(r.getTerm(), -1);
                    }
                } catch (Exception ignored) {
                    // unreachable peer; it will sync from a snapshot later
                }
            });
        }
        boolean committed;
        try {
            committed = quorum.await(REPLICATION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            committed = false;
        }
        WriteResponse.Builder resp = WriteResponse.newBuilder()
                .setIndex(entry.getIndex()).setLeaderId(id).setSuccess(committed);
        if (!committed) resp.setError("no quorum: only " + acks.get() + "/" + clusterSize() + " acks");
        return resp.build();
    }

    /**
     * Forwards a write to the leader without blocking this node's worker thread
     * (a blocked worker pool on followers could otherwise starve replication).
     */
    private void forwardToLeader(OpType op, String key, String value, StreamObserver<WriteResponse> out) {
        int lid = leaderId;
        if (lid < 0 || lid == id || !peerAddrs.containsKey(lid)) {
            out.onNext(WriteResponse.newBuilder().setSuccess(false).setLeaderId(-1)
                    .setError("no leader elected yet, retry").build());
            out.onCompleted();
            return;
        }
        KeyValueGrpc.KeyValueFutureStub stub = KeyValueGrpc.newFutureStub(channel(lid))
                .withDeadlineAfter(REPLICATION_TIMEOUT_MS * 2, TimeUnit.MILLISECONDS);
        com.google.common.util.concurrent.ListenableFuture<WriteResponse> f = op == OpType.PUT
                ? stub.put(PutRequest.newBuilder().setKey(key).setValue(value).build())
                : stub.delete(DeleteRequest.newBuilder().setKey(key).build());
        f.addListener(() -> {
            WriteResponse r;
            try {
                r = f.get();
            } catch (Exception e) {
                r = WriteResponse.newBuilder().setSuccess(false).setLeaderId(lid)
                        .setError("leader unreachable: " + e.getMessage()).build();
            }
            out.onNext(r);
            out.onCompleted();
        }, rpcPool);
    }

    private void syncFromLeader(int lid) {
        if (syncing || lid < 0 || lid == id) return;
        syncing = true;
        rpcPool.submit(() -> {
            try {
                SnapshotResponse snap = repl(lid, 5000).snapshot(Empty.getDefaultInstance());
                store.installSnapshot(snap.getEntriesList(), snap.getLastIndex(), snap.getTerm());
                LOG.info(() -> "node " + id + " installed snapshot up to index " + snap.getLastIndex());
            } catch (Exception e) {
                LOG.warning("node " + id + " snapshot sync failed: " + e.getMessage());
            } finally {
                syncing = false;
            }
        });
    }

    /**
     * Optional simulated storage cost per read (system property kvstore.readCostMicros,
     * default 0). Used by the benchmark to model a disk-backed node; off in normal use.
     */
    private static final long READ_COST_NANOS =
            Long.getLong("kvstore.readCostMicros", 0L) * 1000L;

    private static void simulateReadCost() {
        if (READ_COST_NANOS <= 0) return;
        java.util.concurrent.locks.LockSupport.parkNanos(READ_COST_NANOS);
    }

    // ------------------------------------------------------------------ gRPC services

    private class KeyValueService extends KeyValueGrpc.KeyValueImplBase {
        @Override
        public void get(GetRequest req, StreamObserver<GetResponse> out) {
            simulateReadCost();
            KVStore.Versioned v = store.get(req.getKey());
            GetResponse.Builder b = GetResponse.newBuilder().setServedBy(id);
            if (v != null) b.setFound(true).setValue(v.value()).setVersion(v.version());
            out.onNext(b.build());
            out.onCompleted();
        }

        @Override
        public void put(PutRequest req, StreamObserver<WriteResponse> out) {
            if (req.getKey().isEmpty()) {
                out.onNext(WriteResponse.newBuilder().setSuccess(false).setError("key must not be empty").build());
                out.onCompleted();
                return;
            }
            handleWrite(OpType.PUT, req.getKey(), req.getValue(), out);
        }

        @Override
        public void delete(DeleteRequest req, StreamObserver<WriteResponse> out) {
            handleWrite(OpType.DELETE, req.getKey(), "", out);
        }

        @Override
        public void clusterInfo(Empty req, StreamObserver<ClusterInfoResponse> out) {
            out.onNext(ClusterInfoResponse.newBuilder()
                    .setNodeId(id).setLeaderId(leaderId).setTerm(term)
                    .setCommitIndex(store.lastIndex()).setKeyCount(store.size())
                    .addAllPeers(peerAddrs.values()).build());
            out.onCompleted();
        }
    }

    private class ReplicationService extends ReplicationGrpc.ReplicationImplBase {
        @Override
        public void heartbeat(HeartbeatRequest req, StreamObserver<HeartbeatResponse> out) {
            boolean ok = req.getTerm() >= term;
            if (ok) {
                stepDown(req.getTerm(), req.getLeaderId());
                // Only pull a snapshot if we are behind AND made no progress since the
                // last heartbeat (avoids snapshotting during normal in-flight replication).
                long mine = store.lastIndex();
                if (req.getCommitIndex() > mine && mine == indexAtLastHeartbeat) {
                    syncFromLeader(req.getLeaderId());
                }
                indexAtLastHeartbeat = mine;
            }
            out.onNext(HeartbeatResponse.newBuilder().setOk(ok).setTerm(term)
                    .setLastIndex(store.lastIndex()).build());
            out.onCompleted();
        }

        @Override
        public void replicate(ReplicateRequest req, StreamObserver<ReplicateResponse> out) {
            boolean ok = false;
            if (req.getTerm() >= term) {
                stepDown(req.getTerm(), req.getLeaderId());
                ok = store.apply(req.getEntry());
                if (!ok) syncFromLeader(req.getLeaderId());
            }
            out.onNext(ReplicateResponse.newBuilder().setOk(ok).setTerm(term)
                    .setLastIndex(store.lastIndex()).build());
            out.onCompleted();
        }

        @Override
        public void requestVote(VoteRequest req, StreamObserver<VoteResponse> out) {
            boolean granted = false;
            synchronized (stateLock) {
                if (req.getTerm() > term) {
                    term = req.getTerm();
                    votedFor = -1;
                    role = Role.FOLLOWER;
                }
                boolean upToDate = req.getLastIndex() >= store.lastIndex();
                if (req.getTerm() == term && (votedFor == -1 || votedFor == req.getCandidateId()) && upToDate) {
                    votedFor = req.getCandidateId();
                    granted = true;
                    lastHeartbeat = System.currentTimeMillis();
                }
            }
            out.onNext(VoteResponse.newBuilder().setGranted(granted).setTerm(term).build());
            out.onCompleted();
        }

        @Override
        public void snapshot(Empty req, StreamObserver<SnapshotResponse> out) {
            KVStore.Snapshot snap = store.snapshot();
            out.onNext(SnapshotResponse.newBuilder().setLastIndex(snap.lastIndex())
                    .setTerm(snap.lastTerm()).addAllEntries(snap.entries()).build());
            out.onCompleted();
        }
    }

    // ------------------------------------------------------------------ main

    /**
     * Usage: Node --id 1 --port 50051 --peers 2=localhost:50052,3=localhost:50053 [--data ./data] [--workers 8]
     */
    public static void main(String[] args) throws Exception {
        Map<String, String> a = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) a.put(args[i].replaceFirst("^--", ""), args[i + 1]);
        int id = Integer.parseInt(a.getOrDefault("id", "1"));
        int port = Integer.parseInt(a.getOrDefault("port", "50051"));
        int workers = Integer.parseInt(a.getOrDefault("workers", "8"));
        Path data = Path.of(a.getOrDefault("data", "./data"));
        Map<Integer, String> peers = new HashMap<>();
        String ps = a.getOrDefault("peers", "");
        if (!ps.isBlank()) {
            for (String p : ps.split(",")) {
                String[] kv = p.split("=");
                peers.put(Integer.parseInt(kv[0].trim()), kv[1].trim());
            }
        }
        Node node = new Node(id, port, peers, data, workers).start();
        Runtime.getRuntime().addShutdownHook(new Thread(node::stop));
        node.blockUntilShutdown();
    }
}
