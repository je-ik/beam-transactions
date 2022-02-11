/**
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

import com.google.common.base.Preconditions;
import com.google.protobuf.TextFormat;
import cz.datadriven.beam.transaction.proto.Server.ClientAck;
import cz.datadriven.beam.transaction.proto.Server.Response;
import cz.datadriven.beam.transaction.proto.TransactionClientGrpc;
import cz.datadriven.beam.transaction.proto.TransactionClientGrpc.TransactionClientStub;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

@Slf4j
public class GrpcResponseWrite extends PTransform<PCollection<KV<String, Response>>, PDone> {

  public static GrpcResponseWrite of() {
    return new GrpcResponseWrite();
  }

  @Value
  private static class ChannelWithObserver {
    ManagedChannel channel;
    TransactionClientStub stub;
    StreamObserver<Response> observer;
  }

  static class GrpcResponseWriteFn extends DoFn<KV<String, Response>, Void> {

    private final Map<String, ChannelWithObserver> openChannels = new ConcurrentHashMap<>();

    @StateId("dummy")
    final StateSpec<ValueState<Boolean>> dummySpec = StateSpecs.value();

    @Teardown
    public void tearDown() {
      openChannels.values().forEach(v -> v.getChannel().shutdown());
    }

    @RequiresStableInput
    @ProcessElement
    public void process(@Element KV<String, Response> element) {
      ChannelWithObserver channelWithObserver =
          openChannels.computeIfAbsent(
              element.getKey(),
              k -> {
                String[] parts = element.getKey().split(":", 2);
                Preconditions.checkArgument(parts.length == 2, "Invalid key %s", element.getKey());
                ManagedChannel channel =
                    ManagedChannelBuilder.forAddress(parts[0], Integer.parseInt(parts[1]))
                        .usePlaintext()
                        .build();
                TransactionClientStub stub = TransactionClientGrpc.newStub(channel);
                return new ChannelWithObserver(channel, stub, null);
              });
      if (channelWithObserver.getObserver() == null) {
        channelWithObserver =
            new ChannelWithObserver(
                channelWithObserver.getChannel(),
                channelWithObserver.getStub(),
                newObserver(element.getKey(), channelWithObserver.getStub()));
        openChannels.put(element.getKey(), channelWithObserver);
      }
      if (log.isDebugEnabled()) {
        log.debug(
            "Returning response {} to observer {}",
            TextFormat.shortDebugString(element.getValue()),
            channelWithObserver.getObserver());
      }
      channelWithObserver.getObserver().onNext(element.getValue());
    }

    private StreamObserver<Response> newObserver(String key, TransactionClientStub stub) {
      return stub.stream(
          new StreamObserver<>() {
            @Override
            public void onNext(ClientAck clientAck) {
              // ignored for now
            }

            @Override
            public void onError(Throwable throwable) {
              // FIXME
              openChannels.remove(key).getChannel().shutdown();
              throw new RuntimeException(throwable);
            }

            @Override
            public void onCompleted() {
              openChannels.remove(key).getChannel().shutdown();
            }
          });
    }
  }

  @Override
  public PDone expand(PCollection<KV<String, Response>> input) {
    input.apply(ParDo.of(new GrpcResponseWriteFn()));
    return PDone.in(input.getPipeline());
  }
}