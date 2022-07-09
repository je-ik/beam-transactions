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

import com.google.common.base.MoreObjects;
import cz.datadriven.beam.transaction.proto.InternalOuterClass.Internal;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class TransactionSeqIdAssign
    extends PTransform<PCollection<Internal>, PCollection<Internal>> {

  public static TransactionSeqIdAssign of() {
    return new TransactionSeqIdAssign();
  }

  static class TransactionSeqIdAssignmentFn extends DoFn<KV<Void, Internal>, Internal> {

    @StateId("seqId")
    final StateSpec<ValueState<Long>> seqIdSpec = StateSpecs.value();

    @StateId("seqIdMap")
    final StateSpec<MapState<String, KV<Long, Long>>> seqIdMapSpec = StateSpecs.map();

    @TimerId("cleanup")
    final TimerSpec cleanupTimerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    private final int cleanupInterval;

    public TransactionSeqIdAssignmentFn(int cleanupInterval) {
      this.cleanupInterval = cleanupInterval;
    }

    @ProcessElement
    public void process(
        @Element KV<Void, Internal> element,
        @Timestamp Instant ts,
        @StateId("seqId") final ValueState<Long> seqId,
        @StateId("seqIdMap") MapState<String, KV<Long, Long>> seqIdMap,
        @TimerId("cleanup") Timer cleanupTimer,
        OutputReceiver<Internal> output) {

      Internal request = element.getValue();
      AtomicLong value = new AtomicLong();
      ReadableState<KV<Long, Long>> transactionSeqId =
          seqIdMap.computeIfAbsent(
              request.getTransactionId(),
              tmp -> {
                long nextSeqId = MoreObjects.firstNonNull(seqId.read(), 1000L);
                seqId.write(nextSeqId + 1);
                value.set(nextSeqId);
                return KV.of(nextSeqId, ts.getMillis());
              });
      KV<Long, Long> assigned = transactionSeqId.read();
      final long assignedSeqId;
      if (assigned == null) {
        assignedSeqId = value.get();
        cleanupTimer.offset(Duration.standardSeconds(cleanupInterval)).setRelative();
      } else {
        assignedSeqId = assigned.getKey();
      }
      output.output(request.toBuilder().setSeqId(assignedSeqId).build());
    }

    @OnTimer("cleanup")
    public void onTimer(
        @Timestamp Instant stamp,
        @StateId("seqIdMap") MapState<String, KV<Long, Long>> seqIdMap,
        @TimerId("cleanup") Timer cleanupTimer) {

      long maxAcceptable = stamp.getMillis() - cleanupInterval * 1000L;
      seqIdMap
          .entries()
          .read()
          .forEach(
              e -> {
                if (e.getValue().getValue() < maxAcceptable) {
                  seqIdMap.remove(e.getKey());
                }
              });
      if (stamp.isBefore(Constants.MAX_TIMER)) {
        cleanupTimer.offset(Duration.standardSeconds(cleanupInterval)).setRelative();
      }
    }
  }

  public PCollection<Internal> expand(PCollection<Internal> input) {
    TransactionRunnerOptions opts =
        input.getPipeline().getOptions().as(TransactionRunnerOptions.class);
    int cleanupInterval = opts.getTransactionCleanupIntervalSeconds();
    return input
        .apply(
            MapElements.into(
                    TypeDescriptors.kvs(TypeDescriptors.voids(), TypeDescriptor.of(Internal.class)))
                .via(
                    request -> {
                      final Internal outputRequest;
                      if (request.getTransactionId().isEmpty()) {
                        outputRequest =
                            request.toBuilder().setTransactionId(newTransactionUid()).build();
                      } else {
                        outputRequest = request;
                      }
                      return KV.of(null, outputRequest);
                    }))
        .apply(ParDo.of(new TransactionSeqIdAssignmentFn(cleanupInterval)));
  }

  private String newTransactionUid() {
    return UUID.randomUUID().toString();
  }
}
