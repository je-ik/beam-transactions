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
import cz.datadriven.beam.transaction.proto.InternalOuterClass.Internal;
import cz.datadriven.beam.transaction.proto.InternalOuterClass.Internal.Builder;
import cz.datadriven.beam.transaction.proto.Server.KeyValue;
import cz.datadriven.beam.transaction.proto.Server.Request;
import cz.datadriven.beam.transaction.proto.Server.Request.Type;
import cz.datadriven.beam.transaction.proto.Server.ServerAck;
import cz.datadriven.beam.transaction.proto.TransactionServerGrpc.TransactionServerImplBase;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.UnboundedPerElement;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators.Manual;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;

@Slf4j
@UnboundedPerElement
public class GrpcRequestReadFn extends DoFn<byte[], Internal> {

  private static volatile Server SERVER = null;
  private static final BlockingQueue<Request> OUTPUT_QUEUE = new ArrayBlockingQueue<>(1000);

  static class RequestService extends TransactionServerImplBase {

    @Override
    public StreamObserver<Request> stream(StreamObserver<ServerAck> responseObserver) {
      BlockingQueue<Request> output = OUTPUT_QUEUE;
      return new StreamObserver<>() {
        @Override
        public void onNext(Request request) {
          log.debug("Received request {}", request.getUid());
          try {
            output.put(request);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            responseObserver.onError(e);
          }
          // FIXME: this must be in bundle finalizer
          responseObserver.onNext(
              ServerAck.newBuilder().setUid(request.getUid()).setStatus(200).build());
        }

        @Override
        public void onError(Throwable throwable) {
          log.error("Error reading requests from client.", throwable);
        }

        @Override
        public void onCompleted() {}
      };
    }
  }

  public class GrpcTracker extends RestrictionTracker<String, Void> {

    private final String restriction;

    boolean finished = false;

    public GrpcTracker(String restriction) {
      this.restriction = Objects.requireNonNull(restriction);
    }

    private Server newServer() {
      return ServerBuilder.forPort(getPort()).addService(new RequestService()).build();
    }

    @Override
    public boolean tryClaim(Void position) {
      if (SERVER == null) {
        synchronized (GrpcRequestReadFn.class) {
          SERVER = newServer();
          log.info("Running new server.");
          try {
            SERVER.start();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }
      return !finished;
    }

    @Override
    public String currentRestriction() {
      return restriction;
    }

    @Override
    public SplitResult<String> trySplit(double fractionOfRemainder) {
      finished = true;
      return SplitResult.of(null, restriction);
    }

    @Override
    public void checkDone() throws IllegalStateException {}

    @Override
    public IsBounded isBounded() {
      return IsBounded.UNBOUNDED;
    }
  }

  @Getter private final int port;
  private final SerializableFunction<Request, Instant> watermarkFn;

  GrpcRequestReadFn(
      TransactionRunnerOptions runnerOpts, SerializableFunction<Request, Instant> watermarkFn) {

    this.port = runnerOpts.getRequestPort();
    this.watermarkFn = watermarkFn == null ? tmp -> Instant.now() : watermarkFn;
  }

  @GetInitialRestriction
  public String getInitialRestriction() {
    return UUID.randomUUID().toString();
  }

  @GetRestrictionCoder
  public Coder<String> getRestrictionCoder() {
    return StringUtf8Coder.of();
  }

  @NewTracker
  public GrpcTracker newTracker(@Restriction String restriction) {
    return new GrpcTracker(restriction);
  }

  @GetInitialWatermarkEstimatorState
  public Instant getInitialWatermarkEstimatorState() {
    return Instant.now();
  }

  @NewWatermarkEstimator
  public ManualWatermarkEstimator<Instant> newEstimator(@WatermarkEstimatorState Instant current) {
    return new Manual(current);
  }

  @Teardown
  public void tearDown() {
    if (SERVER != null) {
      synchronized (GrpcRequestReadFn.class) {
        if (SERVER != null) {
          SERVER.shutdown();
          SERVER = null;
        }
      }
    }
  }

  @ProcessElement
  public ProcessContinuation process(
      RestrictionTracker<String, Void> tracker,
      OutputReceiver<Internal> output,
      ManualWatermarkEstimator<Instant> estimator) {

    while (tracker.tryClaim(null)) {
      BlockingQueue<Request> requests = OUTPUT_QUEUE;
      try {
        @Nullable Request polled;
        do {
          polled = requests.poll(10, TimeUnit.MILLISECONDS);
          if (log.isDebugEnabled()) {
            log.debug(
                "Polled from queue {}",
                polled != null ? TextFormat.shortDebugString(polled) : null);
          }
          Instant watermark = watermarkFn.apply(polled);
          estimator.setWatermark(watermark);
          if (polled != null) {
            output.outputWithTimestamp(toInternal(polled), Instant.now());
            if (!watermark.isBefore(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
              break;
            }
          } else if (!watermark.isBefore(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
            return ProcessContinuation.stop();
          }
        } while (polled != null);
        return ProcessContinuation.resume();
      } catch (InterruptedException e) {
        log.info("Interrupted while processing requests.", e);
        return ProcessContinuation.stop();
      }
    }
    return ProcessContinuation.stop();
  }

  private Internal toInternal(Request request) {
    Builder builder =
        Internal.newBuilder().setRequest(request).setTransactionId(request.getTransactionId());
    if (request.getType().equals(Type.READ)) {
      for (String key : request.getReadPayload().getKeyList()) {
        builder.addKeyValue(Internal.KeyValue.newBuilder().setKey(key));
      }
    } else if (request.getType().equals(Type.WRITE)) {
      for (KeyValue kv : request.getWritePayload().getKeyValueList()) {
        builder.addKeyValue(
            Internal.KeyValue.newBuilder().setKey(kv.getKey()).setValue(kv.getValue()));
      }
    }
    return builder.build();
  }
}
