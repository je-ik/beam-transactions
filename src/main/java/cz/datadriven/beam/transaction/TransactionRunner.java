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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.protobuf.GeneratedMessageV3;
import cz.datadriven.beam.transaction.proto.InternalOuterClass.Internal;
import cz.datadriven.beam.transaction.proto.Server;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

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

  private final PipelineOptions opts;

  public TransactionRunner(String[] args) {
    opts = PipelineOptionsFactory.fromArgs(args).create();
  }

  void run() {
    DatabaseAccessor accessor = createAccessor(opts);
    Pipeline pipeline = registerCoders(Pipeline.create(opts));
    PCollection<Internal> requests =
        pipeline.apply(GrpcRequestRead.of()).apply(TransactionSeqIdAssign.of());
    PCollection<Internal> readResponses =
        requests
            .apply(Filter.by(r -> r.getRequest().hasReadPayload()))
            .apply(DatabaseRead.of(accessor));

    PCollection<Internal> resolved =
        PCollectionList.of(readResponses)
            .and(requests.apply(Filter.by(r -> !r.getRequest().hasReadPayload())))
            .apply(Flatten.pCollections());

    resolved
        .apply(
            MapElements.into(
                    TypeDescriptors.kvs(
                        TypeDescriptors.strings(), TypeDescriptor.of(Internal.class)))
                .via(r -> KV.of(r.getRequest().getRequestUid(), r)))
        .apply(GrpcResponseWrite.of());
    pipeline.run();
  }

  private DatabaseAccessor createAccessor(PipelineOptions opts) {
    // FIXME
    return new MemoryDatabaseAccessor();
  }

  @VisibleForTesting
  static Pipeline registerCoders(Pipeline pipeline) {
    Set<GeneratedMessageV3> protos =
        Sets.newHashSet(
            Server.Request.getDefaultInstance(),
            Server.Response.getDefaultInstance(),
            Server.ServerAck.getDefaultInstance(),
            Internal.getDefaultInstance());
    for (GeneratedMessageV3 m : protos) {
      pipeline.getCoderRegistry().registerCoderForClass(m.getClass(), ProtoCoder.of(m));
    }
    return pipeline;
  }
}
