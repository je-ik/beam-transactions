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
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import cz.datadriven.beam.transaction.proto.InternalOuterClass.Internal;
import cz.datadriven.beam.transaction.proto.InternalOuterClass.Internal.KeyValue;
import cz.datadriven.beam.transaction.proto.Server.Request.Type;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Instant;

public class VerifyTransactions extends PTransform<PCollection<Internal>, PCollection<Internal>> {

  public static VerifyTransactions of() {
    return new VerifyTransactions();
  }

  @VisibleForTesting
  static class GatherTransactionRequestsFn extends DoFn<KV<String, Internal>, List<Internal>> {

    @StateId("gathered")
    final StateSpec<BagState<Internal>> gatheredSpec = StateSpecs.bag();

    @TimerId("flushTimer")
    final TimerSpec flushSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @ProcessElement
    public void process(
        @Element KV<String, Internal> element,
        @Timestamp Instant ts,
        @StateId("gathered") BagState<Internal> gathered,
        @TimerId("flushTimer") Timer flushTimer,
        OutputReceiver<List<Internal>> output) {

      gathered.add(element.getValue());
      if (element.getValue().getRequest().getType().equals(Type.COMMIT)) {
        flushTimer.withOutputTimestamp(ts).set(ts);
      } else {
        output.output(Collections.singletonList(element.getValue()));
      }
    }

    @OnTimer("flushTimer")
    public void onFlushTimer(
        @StateId("gathered") BagState<Internal> gathered, OutputReceiver<List<Internal>> output) {

      Iterable<Internal> res = gathered.read();
      output.output(Lists.newArrayList(res));
      gathered.clear();
    }

    @Override
    public TypeDescriptor<List<Internal>> getOutputTypeDescriptor() {
      return TypeDescriptors.lists(TypeDescriptor.of(Internal.class));
    }
  }

  @VisibleForTesting
  static class VerifyTransactionsFn extends DoFn<KV<Void, List<Internal>>, Internal> {

    @StateId("seqId")
    final StateSpec<MapState<String, Long>> lastWriteSeqIdSpec = StateSpecs.map();

    // FIXME: can be rewritten to sort by seqId
    @RequiresStableInput
    @RequiresTimeSortedInput
    @ProcessElement
    public void process(
        @Element KV<Void, List<Internal>> element,
        @Timestamp Instant ts,
        @StateId("seqId") MapState<String, Long> lastWriteSeqId,
        OutputReceiver<Internal> output) {

      List<Internal> actions = Objects.requireNonNull(element.getValue());
      Internal commit = getCommitIfPresent(actions);
      if (commit != null) {
        if (verifyNoConflict(actions, commit.getSeqId(), lastWriteSeqId)) {
          flushWrites(actions, commit.getSeqId(), lastWriteSeqId);
          actions
              .stream()
              .filter(a -> a.getRequest().getType().equals(Type.WRITE))
              .forEach(output::output);
          output.output(commit.toBuilder().setStatus(200).build());
        } else {
          output.output(commit.toBuilder().setStatus(412).build());
        }
      }
    }

    private void flushWrites(
        List<Internal> actions, long seqId, MapState<String, Long> lastWriteSeqId) {

      actions
          .stream()
          .filter(a -> a.getRequest().getType().equals(Type.WRITE))
          .flatMap(a -> a.getKeyValueList().stream())
          .distinct()
          .forEach(kv -> lastWriteSeqId.put(kv.getKey(), seqId));
    }

    private boolean verifyNoConflict(
        List<Internal> actions, long commitSeqId, MapState<String, Long> lastWriteSeqId) {

      // caching
      actions
          .stream()
          .flatMap(a -> a.getKeyValueList().stream().map(KeyValue::getKey))
          .distinct()
          .forEach(k -> lastWriteSeqId.get(k).readLater());
      return actions.stream().allMatch(a -> isValidRead(a, commitSeqId, lastWriteSeqId));
    }

    private boolean isValidRead(
        Internal action, long actionSeqId, MapState<String, Long> lastWriteSeqId) {

      return action
          .getKeyValueList()
          .stream()
          .allMatch(kv -> isValidKvAccess(actionSeqId, lastWriteSeqId, kv));
    }

    private boolean isValidKvAccess(
        long actionSeqId, MapState<String, Long> lastWriteSeqId, KeyValue kv) {

      boolean isRead = kv.getSeqId() != 0;
      long lastWrite = MoreObjects.firstNonNull(lastWriteSeqId.get(kv.getKey()).read(), -1L);
      if (isRead) {
        return kv.getSeqId() == lastWrite || lastWrite == -1L;
      }
      return actionSeqId > lastWrite;
    }

    @Nullable
    private Internal getCommitIfPresent(List<Internal> actions) {
      return actions
          .stream()
          .filter(a -> a.getRequest().getType().equals(Type.COMMIT))
          .findAny()
          .orElse(null);
    }
  }

  @Override
  public PCollection<Internal> expand(PCollection<Internal> input) {
    return input
        .apply(WithKeys.of(Internal::getTransactionId).withKeyType(TypeDescriptors.strings()))
        .apply("gather", ParDo.of(new GatherTransactionRequestsFn()))
        .apply(WithKeys.of((Void) null))
        .apply("verify", ParDo.of(new VerifyTransactionsFn()));
  }
}
