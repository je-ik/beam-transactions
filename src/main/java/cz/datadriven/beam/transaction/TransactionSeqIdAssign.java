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

import com.google.common.base.MoreObjects;
import cz.datadriven.beam.transaction.proto.Server.Request;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

public class TransactionSeqIdAssign
    extends PTransform<PCollection<Request>, PCollection<Request>> {

  public static TransactionSeqIdAssign of() {
    return new TransactionSeqIdAssign();
  }

  static class TransactionSeqIdAssignmentFn extends DoFn<KV<Void, Request>, Request> {

    @StateId("seqId")
    final StateSpec<ValueState<Long>> seqIdSpec = StateSpecs.value();

    @StateId("seqIdMap")
    final StateSpec<MapState<String, Long>> seqIdMapSpec = StateSpecs.map();

    @ProcessElement
    public void process(
        @Element KV<Void, Request> element,
        @StateId("seqId") final ValueState<Long> seqId,
        @StateId("seqIdMap") MapState<String, Long> seqIdMap,
        OutputReceiver<Request> output) {

      Request request = element.getValue();
      AtomicLong value = new AtomicLong();
      ReadableState<Long> transactionSeqId =
          seqIdMap.computeIfAbsent(
              request.getTransactionId(),
              tmp -> {
                long nextSeqId = MoreObjects.firstNonNull(seqId.read(), 1L);
                seqId.write(nextSeqId + 1);
                value.set(nextSeqId);
                return nextSeqId;
              });
      long assignedSeqId = MoreObjects.firstNonNull(transactionSeqId.read(), value.get());
      output.output(request.toBuilder().setSeqId(assignedSeqId).build());
    }
  }

  public PCollection<Request> expand(PCollection<Request> input) {
    return input
        .apply(
            MapElements.into(
                    TypeDescriptors.kvs(TypeDescriptors.voids(), TypeDescriptor.of(Request.class)))
                .via(
                    request -> {
                      final Request outputRequest;
                      if (request.getTransactionId().isEmpty()) {
                        outputRequest =
                            request.toBuilder().setTransactionId(newTransactionUid()).build();
                      } else {
                        outputRequest = request;
                      }
                      return KV.of(null, outputRequest);
                    }))
        .apply(ParDo.of(new TransactionSeqIdAssignmentFn()));
  }

  private String newTransactionUid() {
    return UUID.randomUUID().toString();
  }
}
