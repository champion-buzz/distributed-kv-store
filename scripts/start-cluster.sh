#!/usr/bin/env bash
# Starts a 3-node cluster as separate processes. Stop with: kill $(cat data/*.pid)
set -euo pipefail
cd "$(dirname "$0")/.."
JAR=target/kvstore.jar
[ -f "$JAR" ] || mvn -q -B package -DskipTests
mkdir -p data
PEERS=(1=localhost:50051 2=localhost:50052 3=localhost:50053)
for id in 1 2 3; do
  others=$(printf "%s\n" "${PEERS[@]}" | grep -v "^$id=" | paste -sd, -)
  java -cp "$JAR" com.kvstore.server.Node --id "$id" --port "5005$id" --peers "$others" --data ./data \
    > "data/node-$id.out" 2>&1 &
  echo $! > "data/node-$id.pid"
  echo "node $id started (pid $!)"
done
echo "Connect with: java -cp $JAR com.kvstore.client.Cli localhost:50051,localhost:50052,localhost:50053"
