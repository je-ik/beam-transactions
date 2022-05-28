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

import cz.datadriven.beam.transaction.DatabaseAccessor.Value;
import cz.datadriven.beam.transaction.TestUtils.AfterNCommits;
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
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.net.SocketFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class TransactionRunnerTest {

  ExecutorService executor;
  int port;

  @BeforeEach
  void setup() {
    executor = Executors.newCachedThreadPool();
    port = (int) (Math.random() * 60000 + 1024);
  }

  @Test
  @Timeout(20)
  void testTransactionsEndToEnd()
      throws ExecutionException, InterruptedException, TimeoutException {
    Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs("--requestPort=" + port).create());
    MemoryDatabaseAccessor accessor = new MemoryDatabaseAccessor();
    accessor.set("alice", Value.builder().amount(100.0).seqId(0L).build());
    accessor.set("bob", Value.builder().amount(50.0).seqId(0L).build());
    TransactionRunner runner = new TransactionRunner();
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
    TransactionClient client = TransactionClient.of("localhost", port);
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
                .setType(Type.WRITE)
                .setTransactionId(transactionId)
                .setWritePayload(
                    WritePayload.newBuilder()
                        .addKeyValue(KeyValue.newBuilder().setKey("alice").setValue(50.0))
                        .addKeyValue(KeyValue.newBuilder().setKey("bob").setValue(100.0)))
                .build(),
            5,
            TimeUnit.SECONDS);
    assertNotNull(response);
    response =
        client.sendSync(
            Request.newBuilder().setType(Type.COMMIT).setTransactionId(transactionId).build(),
            5,
            TimeUnit.SECONDS);
    assertEquals(200, response.getStatus());
    assertTrue(err.take().isEmpty());
    assertEquals(State.DONE, result.get().getState());
    assertEquals(50.0, accessor.get("alice").getAmount(), 0.0001);
    assertEquals(100.0, accessor.get("bob").getAmount(), 0.0001);
  }

  @Test
  @Timeout(20)
  void testTransactionsEndToEndWithCommitRejected()
      throws ExecutionException, InterruptedException, TimeoutException {

    Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs("--requestPort=" + port).create());
    MemoryDatabaseAccessor accessor = new MemoryDatabaseAccessor();
    accessor.set("alice", Value.builder().amount(100.0).seqId(0L).build());
    accessor.set("bob", Value.builder().amount(50.0).seqId(0L).build());
    TransactionRunner runner = new TransactionRunner();
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
    TransactionClient client = TransactionClient.of("localhost", port);
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
                .setType(Type.WRITE)
                .setWritePayload(
                    WritePayload.newBuilder()
                        .addKeyValue(KeyValue.newBuilder().setKey("alice").setValue(75.0))
                        .addKeyValue(KeyValue.newBuilder().setKey("bob").setValue(75.0)))
                .build(),
            5,
            TimeUnit.SECONDS);
    String transactionId2 = response.getTransactionId();
    assertFalse(transactionId2.isEmpty());
    response =
        client.sendSync(
            Request.newBuilder().setType(Type.COMMIT).setTransactionId(transactionId2).build(),
            5,
            TimeUnit.SECONDS);
    assertEquals(200, response.getStatus());
    response =
        client.sendSync(
            Request.newBuilder()
                .setType(Type.WRITE)
                .setTransactionId(transactionId1)
                .setWritePayload(
                    WritePayload.newBuilder()
                        .addKeyValue(KeyValue.newBuilder().setKey("alice").setValue(50.0))
                        .addKeyValue(KeyValue.newBuilder().setKey("bob").setValue(100.0)))
                .build(),
            5,
            TimeUnit.SECONDS);
    assertNotNull(response);
    response =
        client.sendSync(
            Request.newBuilder().setType(Type.COMMIT).setTransactionId(transactionId1).build(),
            5,
            TimeUnit.SECONDS);
    assertEquals(412, response.getStatus());
    assertTrue(err.take().isEmpty());
    assertEquals(State.DONE, result.get().getState());
    assertEquals(75.0, accessor.get("alice").getAmount(), 0.0001);
    assertEquals(75.0, accessor.get("bob").getAmount(), 0.0001);
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
}
