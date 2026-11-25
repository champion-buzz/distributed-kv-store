package com.kvstore.server;

import com.kvstore.proto.LogEntry;
import com.kvstore.proto.OpType;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe in-memory key-value state with a write-ahead log (WAL) on disk.
 *
 * Reads are lock-free (ConcurrentHashMap). Writes go through {@link #apply(LogEntry)},
 * which is synchronized so log entries are applied and persisted in index order.
 */
public class KVStore {

    public record Versioned(String value, long version) {}

    private final ConcurrentHashMap<String, Versioned> data = new ConcurrentHashMap<>();
    private final AtomicLong lastIndex = new AtomicLong(0);
    private final AtomicLong lastTerm = new AtomicLong(0);
    private final Path walPath;
    private BufferedWriter wal;

    public KVStore(Path dataDir, int nodeId) throws IOException {
        if (dataDir == null) {
            this.walPath = null;
            return;
        }
        Files.createDirectories(dataDir);
        this.walPath = dataDir.resolve("node-" + nodeId + ".wal");
        replay();
        this.wal = Files.newBufferedWriter(walPath, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    public Versioned get(String key) {
        return data.get(key);
    }

    public long lastIndex() {
        return lastIndex.get();
    }

    public long lastTerm() {
        return lastTerm.get();
    }

    public int size() {
        return data.size();
    }

    /** Applies an entry if it is the next one in sequence. Returns false on a gap. */
    public synchronized boolean apply(LogEntry e) {
        long expected = lastIndex.get() + 1;
        if (e.getIndex() < expected) {
            return true; // duplicate / already applied — idempotent
        }
        if (e.getIndex() > expected) {
            return false; // gap: caller must sync from leader
        }
        applyUnchecked(e);
        persist(e);
        return true;
    }

    /** Leader-side helper: assigns the next index and applies atomically. */
    public synchronized LogEntry appendAsLeader(long term, OpType op, String key, String value) {
        LogEntry e = LogEntry.newBuilder()
                .setIndex(lastIndex.get() + 1)
                .setTerm(term)
                .setOp(op)
                .setKey(key)
                .setValue(value == null ? "" : value)
                .build();
        applyUnchecked(e);
        persist(e);
        return e;
    }

    /** Replaces the whole state with a snapshot taken from the leader. */
    public synchronized void installSnapshot(List<LogEntry> entries, long index, long term) {
        data.clear();
        for (LogEntry e : entries) {
            data.put(e.getKey(), new Versioned(e.getValue(), e.getIndex()));
        }
        lastIndex.set(index);
        lastTerm.set(term);
        rewriteWal();
    }

    public record Snapshot(List<LogEntry> entries, long lastIndex, long lastTerm) {}

    /** Atomic point-in-time snapshot of the state. */
    public synchronized Snapshot snapshot() {
        return new Snapshot(snapshotEntries(), lastIndex.get(), lastTerm.get());
    }

    /** Current state as a list of PUT entries (used for snapshots). */
    public synchronized List<LogEntry> snapshotEntries() {
        List<LogEntry> out = new ArrayList<>(data.size());
        for (Map.Entry<String, Versioned> kv : data.entrySet()) {
            out.add(LogEntry.newBuilder()
                    .setIndex(kv.getValue().version())
                    .setOp(OpType.PUT)
                    .setKey(kv.getKey())
                    .setValue(kv.getValue().value())
                    .build());
        }
        return out;
    }

    private void applyUnchecked(LogEntry e) {
        if (e.getOp() == OpType.PUT) {
            data.put(e.getKey(), new Versioned(e.getValue(), e.getIndex()));
        } else {
            data.remove(e.getKey());
        }
        lastIndex.set(e.getIndex());
        lastTerm.set(e.getTerm());
    }

    // ---------------- WAL ----------------

    private static String enc(String s) {
        return Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    private static String dec(String s) {
        return new String(Base64.getDecoder().decode(s), StandardCharsets.UTF_8);
    }

    private static String line(LogEntry e) {
        return e.getIndex() + " " + e.getTerm() + " " + e.getOp().name() + " "
                + enc(e.getKey()) + " " + enc(e.getValue());
    }

    private void persist(LogEntry e) {
        if (wal == null) return;
        try {
            wal.write(line(e));
            wal.newLine();
            wal.flush();
        } catch (IOException ex) {
            throw new RuntimeException("WAL write failed", ex);
        }
    }

    private void replay() throws IOException {
        if (!Files.exists(walPath)) return;
        try (BufferedReader r = Files.newBufferedReader(walPath, StandardCharsets.UTF_8)) {
            String l;
            while ((l = r.readLine()) != null) {
                String[] p = l.split(" ", -1);
                if (p.length != 5) continue; // torn write at the tail
                LogEntry e = LogEntry.newBuilder()
                        .setIndex(Long.parseLong(p[0]))
                        .setTerm(Long.parseLong(p[1]))
                        .setOp(OpType.valueOf(p[2]))
                        .setKey(dec(p[3]))
                        .setValue(dec(p[4]))
                        .build();
                // a snapshot rewrite writes entries with their original versions, so
                // apply unconditionally and keep the max index seen
                if (e.getOp() == OpType.PUT) {
                    data.put(e.getKey(), new Versioned(e.getValue(), e.getIndex()));
                } else {
                    data.remove(e.getKey());
                }
                if (e.getIndex() > lastIndex.get()) lastIndex.set(e.getIndex());
                if (e.getTerm() > lastTerm.get()) lastTerm.set(e.getTerm());
            }
        }
    }

    private void rewriteWal() {
        if (walPath == null) return;
        try {
            if (wal != null) wal.close();
            Path tmp = walPath.resolveSibling(walPath.getFileName() + ".tmp");
            try (BufferedWriter w = Files.newBufferedWriter(tmp, StandardCharsets.UTF_8)) {
                for (Map.Entry<String, Versioned> kv : data.entrySet()) {
                    w.write(line(LogEntry.newBuilder()
                            .setIndex(kv.getValue().version())
                            .setTerm(lastTerm.get())
                            .setOp(OpType.PUT)
                            .setKey(kv.getKey())
                            .setValue(kv.getValue().value())
                            .build()));
                    w.newLine();
                }
                // marker so lastIndex survives even if the newest key was deleted
                w.write(lastIndex.get() + " " + lastTerm.get() + " DELETE " + enc("__snapshot_marker__") + " " + enc(""));
                w.newLine();
            }
            Files.move(tmp, walPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            wal = Files.newBufferedWriter(walPath, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException ex) {
            throw new RuntimeException("WAL rewrite failed", ex);
        }
    }

    public synchronized void close() {
        try {
            if (wal != null) wal.close();
        } catch (IOException ignored) {
        }
    }
}
