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

import com.google.common.annotations.VisibleForTesting;
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
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TransactionClient implements Closeable {

  public static TransactionClient of(String host, int port) {
    try {
      return of(host, port, InetAddress.getLocalHost().getHostName());
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }

  public static TransactionClient of(String host, int port, String responseHostname) {
    Exception caught = null;
    for (int i = 0; i < 3; i++) {
      try {
        return new TransactionClient(host, port, responseHostname);
      } catch (IOException ex) {
        if (caught == null) {
          caught = ex;
        } else {
          caught.addSuppressed(ex);
        }
      }
    }
    throw new RuntimeException(caught);
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

  private final ManagedChannel channel;
  private final TransactionServerStub stub;
  private final AtomicLong reqId = new AtomicLong();
  private final String responseHostname;
  private final int localPort = new Random().nextInt(60000) + 1024;
  private final Map<String, CompletableFuture<Response>> responseMap = new ConcurrentHashMap<>();
  private final Server server;

  private volatile StreamObserver<Request> requestObserver;

  private TransactionClient(String host, int port, String responseHostname) throws IOException {
    this.channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    this.stub = TransactionServerGrpc.newStub(channel).withWaitForReady();
    this.responseHostname = responseHostname;
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

  public Response sendSync(Request request, long timeout, TimeUnit unit)
      throws ExecutionException, InterruptedException, TimeoutException {

    String requestUid = UUID.randomUUID().toString();
    CompletableFuture<Response> responseFuture = new CompletableFuture<>();
    responseMap.put(requestUid, responseFuture);
    try {
      sendRequestAsync(
          request
              .toBuilder()
              .setRequestUid(requestUid)
              .setResponseHost(responseHostname)
              .setResponsePort(localPort)
              .build());
      return responseFuture.get(timeout, unit);
    } finally {
      responseMap.remove(requestUid);
    }
  }

  @VisibleForTesting
  void sendRequestAsync(Request request) {
    long req = reqId.getAndIncrement();
    Request toSend = request.toBuilder().setUid(req).build();
    synchronized (this) {
      requestObserver().onNext(toSend);
    }
  }

  private StreamObserver<Request> requestObserver() {
    if (requestObserver == null) {
      requestObserver =
          stub.stream(
              new StreamObserver<>() {
                @Override
                public void onNext(ServerAck serverAck) {}

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
