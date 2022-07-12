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

import static org.junit.jupiter.api.Assertions.*;

import com.google.common.base.MoreObjects;
import cz.datadriven.beam.transaction.DatabaseAccessor.Value;
import cz.datadriven.beam.transaction.TestUtils.AfterNCommits;
import cz.datadriven.beam.transaction.TestUtils.StaticBoolean;
import cz.datadriven.beam.transaction.proto.Server.KeyValue;
import cz.datadriven.beam.transaction.proto.Server.ReadPayload;
import cz.datadriven.beam.transaction.proto.Server.Request;
import cz.datadriven.beam.transaction.proto.Server.Request.Type;
import cz.datadriven.beam.transaction.proto.Server.Response;
import cz.datadriven.beam.transaction.proto.Server.WritePayload;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.net.SocketFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Slf4j
public class TransactionRunnerTest {

  ExecutorService executor;
  int port;

  @BeforeEach
  void setup() {
    executor =
        Executors.newCachedThreadPool(
            r -> {
              Thread ret = new Thread(r);
              ret.setUncaughtExceptionHandler(
                  (t, err) -> {
                    log.error("Error in thread {}", t.getName(), err);
                  });
              return ret;
            });
    port = (int) (Math.random() * 60000 + 1024);
  }

  @Test
  @Timeout(20)
  void testTransactionsEndToEnd()
      throws ExecutionException, InterruptedException, TimeoutException {

    Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs("--requestPort=" + port).create());
    MemoryDatabaseAccessor accessor = new MemoryDatabaseAccessor();
    long now = System.currentTimeMillis();
    accessor.set("alice", new Value(100.0, 1L, now));
    accessor.set("bob", new Value(50.0, 1L, now));
    TransactionRunner runner = createRunner(accessor);
    runner.createRunnablePipeline(
        p,
        r -> {
          if (r != null && r.getType().equals(Type.COMMIT)) {
            return BoundedWindow.TIMESTAMP_MAX_VALUE;
          }
          return Instant.now();
        });
    BlockingQueue<Optional<Throwable>> err = new ArrayBlockingQueue<>(1);
    Future<PipelineResult> result =
        executor.submit(
            () -> {
              try {
                PipelineResult res = p.run();
                err.put(Optional.empty());
                return res;
              } catch (Exception ex) {
                err.put(Optional.of(ex));
              }
              return null;
            });
    // wait till server runs
    waitTillPortReady(port);
    try (TransactionClient client = TransactionClient.of("localhost", port)) {
      Response response =
          client.sendSync(
              Request.newBuilder()
                  .setType(Type.READ)
                  .setReadPayload(ReadPayload.newBuilder().addKey("alice").addKey("bob"))
                  .build(),
              5,
              TimeUnit.SECONDS);
      String transactionId = response.getTransactionId();
      assertEquals("alice", response.getKeyvalueList().get(0).getKey());
      assertEquals(100.0, response.getKeyvalueList().get(0).getValue(), 0.001);
      assertEquals("bob", response.getKeyvalueList().get(1).getKey());
      assertEquals(50.0, response.getKeyvalueList().get(1).getValue(), 0.001);
      response =
          client.sendSync(
              Request.newBuilder()
                  .setType(Type.COMMIT)
                  .setTransactionId(transactionId)
                  .setWritePayload(
                      WritePayload.newBuilder()
                          .addKeyValue(KeyValue.newBuilder().setKey("alice").setValue(50.0))
                          .addKeyValue(KeyValue.newBuilder().setKey("bob").setValue(100.0)))
                  .build(),
              5,
              TimeUnit.SECONDS);
      assertEquals(200, response.getStatus());
      assertTrue(err.take().isEmpty());
      assertEquals(State.DONE, result.get().getState());
      assertEquals(50.0, accessor.get("alice").getAmount(), 0.0001);
      assertEquals(100.0, accessor.get("bob").getAmount(), 0.0001);
    }
  }

  @Test
  @Timeout(30)
  void testTransactionsEndToEndWithCommitRejected()
      throws ExecutionException, InterruptedException, TimeoutException {

    Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs("--requestPort=" + port).create());
    MemoryDatabaseAccessor accessor = new MemoryDatabaseAccessor();
    long now = System.currentTimeMillis();
    accessor.set("alice", new Value(100.0, 1L, now));
    accessor.set("bob", new Value(50.0, 1L, now));
    TransactionRunner runner = createRunner(accessor);
    runner.createRunnablePipeline(p, afterCommits(2));
    BlockingQueue<Optional<Throwable>> err = new ArrayBlockingQueue<>(1);
    Future<PipelineResult> result =
        executor.submit(
            () -> {
              try {
                PipelineResult res = p.run();
                err.put(Optional.empty());
                return res;
              } catch (Exception ex) {
                err.put(Optional.of(ex));
              }
              return null;
            });
    // wait till server runs
    waitTillPortReady(port);
    try (TransactionClient client = TransactionClient.of("localhost", port)) {
      Response response =
          client.sendSync(
              Request.newBuilder()
                  .setType(Type.READ)
                  .setReadPayload(ReadPayload.newBuilder().addKey("alice").addKey("bob"))
                  .build(),
              5,
              TimeUnit.SECONDS);
      String transactionId1 = response.getTransactionId();
      response =
          client.sendSync(
              Request.newBuilder()
                  .setType(Type.READ)
                  .setReadPayload(ReadPayload.newBuilder().addKey("dummy"))
                  .build(),
              5,
              TimeUnit.SECONDS);
      String transactionId2 = response.getTransactionId();
      assertFalse(transactionId2.isEmpty());
      assertNotNull(response);
      response =
          client.sendSync(
              Request.newBuilder()
                  .setType(Type.COMMIT)
                  .setTransactionId(transactionId2)
                  .setWritePayload(
                      WritePayload.newBuilder()
                          .addKeyValue(KeyValue.newBuilder().setKey("alice").setValue(75.0))
                          .addKeyValue(KeyValue.newBuilder().setKey("bob").setValue(75.0)))
                  .build(),
              5,
              TimeUnit.SECONDS);
      assertEquals(200, response.getStatus());
      response =
          client.sendSync(
              Request.newBuilder()
                  .setType(Type.COMMIT)
                  .setTransactionId(transactionId1)
                  .setWritePayload(
                      WritePayload.newBuilder()
                          .addKeyValue(KeyValue.newBuilder().setKey("alice").setValue(50.0))
                          .addKeyValue(KeyValue.newBuilder().setKey("bob").setValue(100.0)))
                  .build(),
              5,
              TimeUnit.SECONDS);
      assertEquals(412, response.getStatus());
      assertTrue(err.take().isEmpty());
      assertEquals(State.DONE, result.get().getState());
      assertEquals(75.0, accessor.get("alice").getAmount(), 0.0001);
      assertEquals(75.0, accessor.get("bob").getAmount(), 0.0001);
    }
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testTransactionsConsistencyDirect() throws InterruptedException, ExecutionException {
    PipelineOptions opts =
        PipelineOptionsFactory.fromArgs("--requestPort=" + port, "--runner=direct").create();
    testTransactionConsistencyWithRunner(2000, opts);
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  @Disabled
  void testTransactionsConsistencyFlink() throws InterruptedException, ExecutionException {
    PipelineOptions opts =
        PipelineOptionsFactory.fromArgs(
                "--requestPort=" + port,
                "--numInitialSplits=10",
                "--runner=flink",
                "--checkpointingInterval=10",
                // "--maxBundleSize=100",
                "--maxBundleTimeMills=10",
                "--parallelism=8",
                "--shutdownSourcesAfterIdleMs=240000")
            .create();
    testTransactionConsistencyWithRunner(10000, opts);
  }

  void testTransactionConsistencyWithRunner(int numTransfers, PipelineOptions opts)
      throws InterruptedException, ExecutionException {

    Pipeline p = Pipeline.create(opts);
    MemoryDatabaseAccessor accessor = new MemoryDatabaseAccessor();
    int clients = 1000;
    int parallelism = 25;
    TransactionRunner runner = createRunner(accessor);
    StaticBoolean finished = new StaticBoolean();
    runner.createRunnablePipeline(
        p, r -> finished.get() ? BoundedWindow.TIMESTAMP_MAX_VALUE : Instant.now());
    CompletableFuture<State> err = new CompletableFuture<>();
    executor.submit(
        () -> {
          try {
            PipelineResult res = p.run();
            err.complete(res.waitUntilFinish());
          } catch (Throwable ex) {
            err.completeExceptionally(ex);
          }
          return null;
        });
    // wait till server runs
    waitTillPortReady(port);
    CountDownLatch finishedLatch = new CountDownLatch(parallelism);
    AtomicInteger committedTransactions = new AtomicInteger();
    for (int j = 0; j < parallelism; j++) {
      executor.submit(
          () -> {
            try (TransactionClient client = TransactionClient.of("localhost", port)) {
              for (int i = 0; i < numTransfers / parallelism; ) {
                String from = pick(ThreadLocalRandom.current(), "client", clients);
                String to = pick(ThreadLocalRandom.current(), "client", clients);
                if (from.equals(to)) {
                  continue;
                }
                double amount = ThreadLocalRandom.current().nextDouble() * 100;
                Response response =
                    client.sendSync(
                        Request.newBuilder()
                            .setType(Type.READ)
                            .setReadPayload(ReadPayload.newBuilder().addKey(from).addKey(to))
                            .build(),
                        20,
                        TimeUnit.SECONDS);
                Map<String, Double> current =
                    response
                        .getKeyvalueList()
                        .stream()
                        .collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue));
                String transactionId = response.getTransactionId();
                double nextFrom = current.get(from) - amount;
                double nextTo = current.get(to) + amount;
                response =
                    client.sendSync(
                        Request.newBuilder()
                            .setType(Type.COMMIT)
                            .setTransactionId(transactionId)
                            .setWritePayload(
                                WritePayload.newBuilder()
                                    .addKeyValue(
                                        KeyValue.newBuilder().setKey(from).setValue(nextFrom))
                                    .addKeyValue(KeyValue.newBuilder().setKey(to).setValue(nextTo)))
                            .build(),
                        20,
                        TimeUnit.SECONDS);
                if (response.getStatus() != 200) {
                  continue;
                }
                committedTransactions.incrementAndGet();
                i++;
              }
            } catch (Exception e) {
              log.error("Failed to process transactions.", e);
              throw new IllegalStateException(e);
            } finally {
              finishedLatch.countDown();
            }
          });
    }
    finishedLatch.await();
    finished.set();
    assertEquals(State.DONE, err.get());
    assertEquals((numTransfers / parallelism) * parallelism, committedTransactions.get());
    double sum = 0.0;
    int nonZeroAmounts = 0;
    Value zero = new Value(0.0, 0L, Long.MIN_VALUE);
    for (int i = 0; i < clients; i++) {
      Value value = MoreObjects.firstNonNull(accessor.get("client" + i), zero);
      sum += value.getAmount();
      if (value.getSeqId() > 0) {
        nonZeroAmounts++;
      }
    }
    assertEquals(0.0, sum, 0.0001);
    assertTrue(nonZeroAmounts > Math.min(numTransfers, clients / 2));
  }

  private TransactionRunner createRunner(MemoryDatabaseAccessor accessor) {
    return new TransactionRunner() {
      @Override
      DatabaseAccessor createAccessor(PipelineOptions opts) {
        return accessor;
      }
    };
  }

  private static SerializableFunction<Request, Instant> afterCommits(int n) {
    return new AfterNCommits(n);
  }

  private void waitTillPortReady(int port) throws InterruptedException {
    while (true) {
      try {
        try (Socket s = SocketFactory.getDefault().createSocket()) {
          s.connect(new InetSocketAddress(InetAddress.getLocalHost(), port));
        }
        break;
      } catch (IOException e) {
        TimeUnit.MILLISECONDS.sleep(100);
      }
    }
  }

  private String pick(Random random, String prefix, int clients) {
    return prefix + random.nextInt(clients);
  }
}
