/*
 * Copyright 2022-2022
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.datadriven.beam.transaction;

import com.codahale.metrics.Reservoir;
import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.codahale.metrics.Snapshot;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import cz.datadriven.beam.transaction.proto.Server.KeyValue;
import cz.datadriven.beam.transaction.proto.Server.ReadPayload;
import cz.datadriven.beam.transaction.proto.Server.Request;
import cz.datadriven.beam.transaction.proto.Server.Request.Type;
import cz.datadriven.beam.transaction.proto.Server.Response;
import cz.datadriven.beam.transaction.proto.Server.WritePayload;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TransactionGenerator {

  private static final int REPORT_INTERVAL = 5;

  public static void main(String[] args) {
    if (args.length < 2) {
      usage();
    }
    final String address = args[0];
    final String responseHost = args[1];
    final int numClients = getInt(args, 2, 5 * Runtime.getRuntime().availableProcessors());
    final int numKeys = getInt(args, 3, 100000);
    final int transactionTimeoutSeconds = getInt(args, 4, 20);
    TransactionGenerator generator =
        new TransactionGenerator(
            address, responseHost, numClients, numKeys, transactionTimeoutSeconds);
    generator.run();
  }

  private static int getInt(String[] args, int pos, int defVal) {
    if (args.length > pos) {
      return Integer.parseInt(args[pos]);
    }
    return defVal;
  }

  private static void usage() {
    System.err.printf(
        "Usage: %s <grpc_address> <response_host>\n", TransactionGenerator.class.getName());
    System.exit(1);
  }

  private final String host;
  private final int port;
  private final String responseHost;
  private final int numClients;
  private final int numKeys;
  private int transactionTimeoutSeconds;

  private final SlidingTimeWindowReservoir latency =
      new SlidingTimeWindowReservoir(30, TimeUnit.SECONDS);
  private final SlidingTimeWindowReservoir committed =
      new SlidingTimeWindowReservoir(30, TimeUnit.SECONDS);
  private final SlidingTimeWindowReservoir rejected =
      new SlidingTimeWindowReservoir(30, TimeUnit.SECONDS);

  public TransactionGenerator(
      String address,
      String responseHost,
      int numClients,
      int numKeys,
      int transactionTimeoutSeconds) {

    String[] parts = address.split(":");
    Preconditions.checkArgument(parts.length == 2, "Invalid host:port %s", address);
    this.host = parts[0];
    this.port = Integer.parseInt(parts[1]);
    this.responseHost = responseHost;
    this.numClients = numClients;
    this.numKeys = numKeys;
    this.transactionTimeoutSeconds = transactionTimeoutSeconds;
  }

  private void run() {
    ScheduledExecutorService executor = Executors.newScheduledThreadPool(numClients + 1);
    executor.scheduleAtFixedRate(
        this::printMetrics, REPORT_INTERVAL, REPORT_INTERVAL, TimeUnit.SECONDS);
    for (int i = 0; i < numClients; i++) {
      executor.execute(this::runClient);
    }
  }

  private void printMetrics() {
    final Snapshot latencySnapshot = latency.getSnapshot();
    final Snapshot committedSnapshot = committed.getSnapshot();
    final Snapshot rejectedSnapshot = rejected.getSnapshot();
    log.info(
        "Latency avg, stddev, median, 99th pct, 99.9th pct:{} {} {} {} {}",
        latencySnapshot.getMean(),
        latencySnapshot.getStdDev(),
        latencySnapshot.getMedian(),
        latencySnapshot.get99thPercentile(),
        latencySnapshot.get999thPercentile());
    log.info("Committed avg per sec {}", committedSnapshot.getValues().length / 30.);
    log.info("Rejected avg per sec {}", rejectedSnapshot.getValues().length / 30.);
  }

  private void runClient() {
    ThreadLocalRandom random = ThreadLocalRandom.current();
    try (TransactionClient client = TransactionClient.of(host, port, responseHost)) {
      for (; ; ) {
        String source = pick(random);
        String target = pick(random);
        double amount = random.nextDouble() * 100.0;
        if (source.equals(target)) {
          continue;
        }
        try {
          long startMs = System.currentTimeMillis();
          transfer(source, target, amount, client);
          long duration = System.currentTimeMillis() - startMs;
          latency.update(duration);
        } catch (Exception ex) {
          log.error("Exception processing transaction", ex);
        }
      }
    }
  }

  private void transfer(String source, String target, double amount, TransactionClient client) {
    for (; ; ) {
      try {
        Response response =
            client.sendSync(
                Request.newBuilder()
                    .setType(Type.READ)
                    .setReadPayload(ReadPayload.newBuilder().addKey(source).addKey(target))
                    .build(),
                transactionTimeoutSeconds,
                TimeUnit.SECONDS);
        String transactionId = response.getTransactionId();
        Map<String, Double> result =
            response
                .getKeyvalueList()
                .stream()
                .collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue));
        double sourceAmount = result.get(source) - amount;
        double targetAmount = result.get(target) + amount;
        response =
            client.sendSync(
                Request.newBuilder()
                    .setTransactionId(transactionId)
                    .setType(Type.COMMIT)
                    .setWritePayload(
                        WritePayload.newBuilder()
                            .addKeyValue(
                                KeyValue.newBuilder().setKey(source).setValue(sourceAmount))
                            .addKeyValue(
                                KeyValue.newBuilder().setKey(target).setValue(targetAmount)))
                    .build(),
                transactionTimeoutSeconds,
                TimeUnit.SECONDS);
        if (response.getStatus() == 200) {
          increaseCommitted();
          log.debug("Transaction {} committed", transactionId);
          break;
        }
        increaseRejected();
        log.debug("Transaction {} rejected. Retrying.", transactionId);
      } catch (Exception ex) {
        log.warn("Error in transaction", ex);
      }
    }
  }

  @VisibleForTesting
  void increaseCommitted() {
    increase(committed);
  }

  @VisibleForTesting
  void increaseRejected() {
    increase(rejected);
  }

  @VisibleForTesting
  Snapshot getCommittedSnapshot() {
    return committed.getSnapshot();
  }

  @VisibleForTesting
  Snapshot getRejectedSnapshot() {
    return rejected.getSnapshot();
  }

  private void increase(Reservoir which) {
    which.update(1);
  }

  private String pick(Random random) {
    return "user" + random.nextInt(numKeys);
  }
}
