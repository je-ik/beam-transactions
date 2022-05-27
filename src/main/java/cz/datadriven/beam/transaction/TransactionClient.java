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

import com.google.protobuf.TextFormat;
import cz.datadriven.beam.transaction.proto.Server.ClientAck;
import cz.datadriven.beam.transaction.proto.Server.Request;
import cz.datadriven.beam.transaction.proto.Server.Response;
import cz.datadriven.beam.transaction.proto.Server.ServerAck;
import cz.datadriven.beam.transaction.proto.TransactionClientGrpc.TransactionClientImplBase;
import cz.datadriven.beam.transaction.proto.TransactionServerGrpc;
import cz.datadriven.beam.transaction.proto.TransactionServerGrpc.TransactionServerStub;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TransactionClient implements Closeable {

  public static TransactionClient of(String host, int port) {
    try {
      return new TransactionClient(host, port);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  class TransactionClientService extends TransactionClientImplBase {
    @Override
    public StreamObserver<Response> stream(StreamObserver<ClientAck> responseObserver) {
      return new StreamObserver<>() {
        @Override
        public void onNext(Response response) {
          if (log.isDebugEnabled()) {
            log.info("Received response {}", TextFormat.shortDebugString(response));
          }
          Optional.ofNullable(responseMap.remove(response.getRequestUid()))
              .ifPresent(f -> f.complete(response));
        }

        @Override
        public void onError(Throwable throwable) {
          throw new RuntimeException(throwable);
        }

        @Override
        public void onCompleted() {}
      };
    }
  }

  private final String host;
  private final int port;
  private final ManagedChannel channel;
  private final TransactionServerStub stub;
  private final AtomicLong reqId = new AtomicLong();
  private final Map<Long, CompletableFuture<ServerAck>> uncompleted = new ConcurrentHashMap<>();
  private final String localHost;
  private final int localPort = new Random().nextInt(60000) + 1024;
  private final Map<String, CompletableFuture<Response>> responseMap = new ConcurrentHashMap<>();
  private final Server server;

  private StreamObserver<Request> requestObserver;

  private TransactionClient(String host, int port) throws IOException {
    this.host = host;
    this.port = port;
    this.channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    this.stub = TransactionServerGrpc.newStub(channel).withWaitForReady();
    this.localHost = InetAddress.getLocalHost().getHostName();
    this.server =
        ServerBuilder.forPort(localPort).addService(new TransactionClientService()).build();
    this.server.start();
  }

  @Override
  public void close() {
    log.info("Closing {}", getClass().getSimpleName());
    if (!channel.isShutdown()) {
      channel.shutdown();
    }
    if (!server.isShutdown()) {
      server.shutdown();
    }
  }

  public Response sendSync(Request request) throws ExecutionException, InterruptedException {
    try {
      return sendSync(request, 0, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  public Response sendSync(Request request, long timeout, TimeUnit unit)
      throws ExecutionException, InterruptedException, TimeoutException {

    String requestUid = UUID.randomUUID().toString();
    CompletableFuture<Response> responseFuture = new CompletableFuture<>();
    responseMap.put(requestUid, responseFuture);
    try {
      Future<ServerAck> ack =
          sendRequestAsync(
              request
                  .toBuilder()
                  .setRequestUid(requestUid)
                  .setResponseHost(localHost)
                  .setResponsePort(localPort)
                  .build());
      ServerAck serverAck = ack.get();
      if (serverAck.getStatus() != 200) {
        throw new IllegalStateException(
            "Received invalid server ack " + TextFormat.shortDebugString(serverAck));
      }
      return responseFuture.get(timeout, unit);
    } finally {
      responseMap.remove(requestUid);
    }
  }

  public Future<ServerAck> sendRequestAsync(Request request) {
    CompletableFuture<ServerAck> ret = new CompletableFuture<>();
    long req = reqId.getAndIncrement();
    uncompleted.put(req, ret);
    Request toSend = request.toBuilder().setUid(req).build();
    synchronized (this) {
      requestObserver().onNext(toSend);
    }
    return ret;
  }

  private StreamObserver<Request> requestObserver() {
    if (requestObserver == null) {
      requestObserver =
          stub.stream(
              new StreamObserver<>() {
                @Override
                public void onNext(ServerAck serverAck) {
                  if (log.isDebugEnabled()) {
                    log.info("Received response {}", TextFormat.shortDebugString(serverAck));
                  }
                  Optional.ofNullable(uncompleted.remove(serverAck.getUid()))
                      .ifPresent(f -> f.complete(serverAck));
                }

                @Override
                public void onError(Throwable throwable) {
                  log.warn("Error on call.", throwable);
                  requestObserver = null;
                }

                @Override
                public void onCompleted() {
                  requestObserver = null;
                }
              });
    }
    return requestObserver;
  }
}
