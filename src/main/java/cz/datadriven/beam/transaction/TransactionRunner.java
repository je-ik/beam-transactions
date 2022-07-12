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
import com.google.common.collect.Sets;
import com.google.protobuf.GeneratedMessageV3;
import cz.datadriven.beam.transaction.proto.InternalOuterClass.Internal;
import cz.datadriven.beam.transaction.proto.Server;
import cz.datadriven.beam.transaction.proto.Server.Request;
import java.io.IOException;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkStateBackendFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.joda.time.Instant;

/**
 * A runner for the transaction {@link org.apache.beam.sdk.Pipeline}.
 *
 * <p>The Pipeline runs a gRPC server for request ingestion, processes it and returns response to a
 * gRPC sink.
 */
public class TransactionRunner {

  public static void main(String[] args) {
    TransactionRunner app = new TransactionRunner(args);
    app.run();
  }

  public static class IncrementalStateBackendFactory implements FlinkStateBackendFactory {

    @Override
    public StateBackend createStateBackend(FlinkPipelineOptions options) {
      try {
        return new RocksDBStateBackend(options.getStateBackendStoragePath(), true);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static class BufferUntilCheckpoint<T> extends PTransform<PCollection<T>, PCollection<T>> {

    static <T> BufferUntilCheckpoint<T> of() {
      return new BufferUntilCheckpoint<>();
    }

    private static class BufferUntilCheckpointFn<T> extends DoFn<T, T> {
      @RequiresStableInput
      @ProcessElement
      public void process(@Element T in, OutputReceiver<T> out) {
        out.output(in);
        ;
      }
    }

    @Override
    public PCollection<T> expand(PCollection<T> input) {
      Coder<T> inputCoder = input.getCoder();
      TypeDescriptor<T> inputTypeDescriptor = input.getTypeDescriptor();
      return input
          .apply(ParDo.of(new BufferUntilCheckpointFn<>()))
          .setTypeDescriptor(inputTypeDescriptor)
          .setCoder(inputCoder);
    }
  }

  private final PipelineOptions opts;

  @VisibleForTesting
  TransactionRunner() {
    this(new String[] {});
  }

  public TransactionRunner(String[] args) {
    opts = PipelineOptionsFactory.fromArgs(args).create();
    setupStateBackend(opts);
  }

  private void setupStateBackend(PipelineOptions opts) {
    TransactionRunnerOptions runnerOpts = opts.as(TransactionRunnerOptions.class);
    if (runnerOpts.getUseIncrementalCheckpoints()) {
      FlinkPipelineOptions flinkOpts = opts.as(FlinkPipelineOptions.class);
      flinkOpts.setStateBackendFactory(IncrementalStateBackendFactory.class);
    }
  }

  void run() {
    Pipeline pipeline = Pipeline.create(opts);
    createRunnablePipeline(pipeline);
    pipeline.run();
  }

  private void createRunnablePipeline(Pipeline pipeline) {
    createRunnablePipeline(pipeline, null);
  }

  @VisibleForTesting
  void createRunnablePipeline(
      Pipeline pipeline, @Nullable SerializableFunction<Request, Instant> watermarkFn) {

    DatabaseAccessor accessor = createAccessor(opts);
    registerCoders(pipeline);
    PCollection<Internal> requests =
        pipeline
            .apply("grpcRead", GrpcRequestRead.of(watermarkFn))
            .apply("assignSeqId", TransactionSeqIdAssign.of());

    PCollection<Internal> readResponses =
        requests
            .apply(Filter.by(r -> r.getRequest().hasReadPayload()))
            .apply("databaseRead", DatabaseRead.of(accessor));

    writeResponses("writeReadResponses", false, readResponses);

    PCollection<Internal> verified =
        PCollectionList.of(readResponses)
            .and(requests.apply(Filter.by(r -> !r.getRequest().hasReadPayload())))
            .apply("flattenReadsAndWrites", Flatten.pCollections())
            .apply("verify", VerifyTransactions.of())
            .apply("commitOutputs", BufferUntilCheckpoint.of());

    verified
        .apply(Filter.by(i -> i.getStatus() == 200))
        .apply("databaseWrite", DatabaseWrite.of(accessor));

    writeResponses("writeCommitResponses", true, verified);
  }

  private void writeResponses(String name, boolean stable, PCollection<Internal> responses) {
    responses
        .apply(
            WithKeys.<String, Internal>of(
                    r -> r.getRequest().getResponseHost() + ":" + r.getRequest().getResponsePort())
                .withKeyType(TypeDescriptors.strings()))
        .apply(name, GrpcResponseWrite.of(stable));
  }

  @VisibleForTesting
  DatabaseAccessor createAccessor(PipelineOptions opts) {
    TransactionRunnerOptions runnerOpts = opts.as(TransactionRunnerOptions.class);
    return new CassandraDatabaseAccessor(
        runnerOpts.getCassandraAuthority(),
        runnerOpts.getCassandraKeyspace(),
        runnerOpts.getCassandraTable());
  }

  @VisibleForTesting
  static void registerCoders(Pipeline pipeline) {
    Set<GeneratedMessageV3> protos =
        Sets.newHashSet(
            Server.Request.getDefaultInstance(),
            Server.Response.getDefaultInstance(),
            Server.ServerAck.getDefaultInstance(),
            Internal.getDefaultInstance());
    for (GeneratedMessageV3 m : protos) {
      pipeline.getCoderRegistry().registerCoderForClass(m.getClass(), ProtoCoder.of(m));
    }
  }
}
