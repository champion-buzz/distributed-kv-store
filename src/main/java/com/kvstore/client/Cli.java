package com.kvstore.client;

import com.kvstore.proto.ClusterInfoResponse;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

/**
 * Interactive shell.
 * Usage: Cli localhost:50051,localhost:50052,localhost:50053
 * Commands: put KEY VALUE | get KEY | del KEY | info | quit
 */
public class Cli {
    public static void main(String[] args) {
        String nodes = args.length > 0 ? args[0] : "localhost:50051,localhost:50052,localhost:50053";
        List<String> addrs = Arrays.asList(nodes.split(","));
        try (KVClient c = new KVClient(addrs, KVClient.Strategy.ROUND_ROBIN);
             Scanner in = new Scanner(System.in)) {
            System.out.println("Connected to " + addrs + ". Commands: put K V | get K | del K | info | quit");
            while (true) {
                System.out.print("kv> ");
                if (!in.hasNextLine()) break;
                String[] p = in.nextLine().trim().split("\\s+", 3);
                if (p[0].isEmpty()) continue;
                switch (p[0].toLowerCase()) {
                    case "put" -> System.out.println(p.length == 3 && c.put(p[1], p[2]) ? "OK" : "FAILED");
                    case "get" -> System.out.println(p.length >= 2 ? c.get(p[1]).orElse("(nil)") : "usage: get KEY");
                    case "del" -> System.out.println(p.length >= 2 && c.delete(p[1]) ? "OK" : "FAILED");
                    case "info" -> {
                        for (ClusterInfoResponse i : c.clusterInfo()) {
                            System.out.printf("node %d  leader=%d  term=%d  index=%d  keys=%d%n",
                                    i.getNodeId(), i.getLeaderId(), i.getTerm(), i.getCommitIndex(), i.getKeyCount());
                        }
                    }
                    case "quit", "exit" -> { return; }
                    default -> System.out.println("unknown command");
                }
            }
        }
    }
}
