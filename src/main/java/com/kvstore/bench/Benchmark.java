package com.kvstore.bench;

import com.kvstore.client.KVClient;
import com.kvstore.server.LocalCluster;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

/**
 * Load test: starts a local 3-node cluster, preloads keys, then hammers it with
 * many concurrent client threads (90% reads / 10% writes) under each read-routing
 * strategy and reports latency percentiles and throughput.
 *
 * Usage: Benchmark [threads=64] [opsPerThread=500] [workersPerNode=4]
 * Tip:   -Dkvstore.readCostMicros=500 models a disk-backed node.
 */
public class Benchmark {

    public static void main(String[] args) throws Exception {
        int threads = args.length > 0 ? Integer.parseInt(args[0]) : 64;
        int opsPerThread = args.length > 1 ? Integer.parseInt(args[1]) : 500;
        int workers = args.length > 2 ? Integer.parseInt(args[2]) : 4;
        int keys = 1000;

        Path dir = Files.createTempDirectory("kvbench");
        try (LocalCluster cluster = new LocalCluster(3, 51051, dir, workers)) {
            cluster.awaitLeader(5000);
            try (KVClient loader = new KVClient(cluster.addresses(), KVClient.Strategy.ROUND_ROBIN)) {
                for (int i = 0; i < keys; i++) loader.put("key-" + i, "value-" + i);
            }
            Thread.sleep(500); // let followers settle

            System.out.printf("threads=%d ops/thread=%d workers/node=%d readCost=%sus%n",
                    threads, opsPerThread, workers, System.getProperty("kvstore.readCostMicros", "0"));
            System.out.printf("%-18s %10s %10s %10s %12s%n", "strategy", "p50(ms)", "p99(ms)", "mean(ms)", "ops/sec");

            // warm-up so JIT/connection setup doesn't skew the first strategy
            run(cluster, KVClient.Strategy.ROUND_ROBIN, threads, opsPerThread / 5, keys);

            double baseline = 0;
            for (KVClient.Strategy s : KVClient.Strategy.values()) {
                Result r = run(cluster, s, threads, opsPerThread, keys);
                if (s == KVClient.Strategy.SINGLE_NODE) baseline = r.mean;
                String delta = s == KVClient.Strategy.SINGLE_NODE ? "" :
                        String.format("   (mean latency %+.0f%% vs single node)", (r.mean - baseline) / baseline * 100);
                System.out.printf("%-18s %10.2f %10.2f %10.2f %12.0f%s%n", s, r.p50, r.p99, r.mean, r.throughput, delta);
            }
        }
        System.exit(0);
    }

    record Result(double p50, double p99, double mean, double throughput) {}

    static Result run(LocalCluster cluster, KVClient.Strategy strategy, int threads, int ops, int keys)
            throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<long[]>> futures = new ArrayList<>();
        CountDownLatch go = new CountDownLatch(1);
        try (KVClient client = new KVClient(cluster.addresses(), strategy)) {
            for (int t = 0; t < threads; t++) {
                futures.add(pool.submit(() -> {
                    long[] lat = new long[ops];
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();
                    go.await();
                    for (int i = 0; i < ops; i++) {
                        String k = "key-" + rnd.nextInt(keys);
                        long t0 = System.nanoTime();
                        if (rnd.nextInt(10) == 0) client.put(k, "v" + i);
                        else client.get(k);
                        lat[i] = System.nanoTime() - t0;
                    }
                    return lat;
                }));
            }
            long start = System.nanoTime();
            go.countDown();
            long[] all = new long[threads * ops];
            int p = 0;
            for (Future<long[]> f : futures) for (long v : f.get()) all[p++] = v;
            double secs = (System.nanoTime() - start) / 1e9;
            pool.shutdown();
            Arrays.sort(all);
            double mean = Arrays.stream(all).average().orElse(0) / 1e6;
            return new Result(all[all.length / 2] / 1e6, all[(int) (all.length * 0.99)] / 1e6, mean, all.length / secs);
        }
    }
}
