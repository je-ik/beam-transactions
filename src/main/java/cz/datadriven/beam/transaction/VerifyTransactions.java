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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
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
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
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
    final StateSpec<MapState<String, KV<Long, Long>>> lastWriteSeqIdSpec = StateSpecs.map();

    // FIXME: SortedMapState would be mode efficient here, but has limited support
    @StateId("sortBuffer")
    final StateSpec<BagState<TimestampedValue<List<Internal>>>> sortBufferSpec = StateSpecs.bag();

    @StateId("minBufferStamp")
    final StateSpec<ValueState<Instant>> minBufferStampSpec = StateSpecs.value();

    @TimerId("sortTimer")
    final TimerSpec sortTimerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    private final long cleanupIntervalMs;

    public VerifyTransactionsFn(long cleanupIntervalMs) {
      this.cleanupIntervalMs = cleanupIntervalMs;
    }

    @ProcessElement
    public void process(
        @Element KV<Void, List<Internal>> element,
        @Timestamp Instant ts,
        @StateId("sortBuffer") BagState<TimestampedValue<List<Internal>>> sortBuffer,
        @StateId("minBufferStamp") ValueState<Instant> minBufferStamp,
        @TimerId("sortTimer") Timer sortTimer) {

      sortBuffer.add(TimestampedValue.of(element.getValue(), ts));
      Instant currentMinStamp =
          MoreObjects.firstNonNull(minBufferStamp.read(), BoundedWindow.TIMESTAMP_MAX_VALUE);
      if (currentMinStamp.isAfter(ts)) {
        sortTimer.withOutputTimestamp(ts).set(ts.plus(1));
        minBufferStamp.write(ts);
      }
    }

    @OnTimer("sortTimer")
    public void onSortTimer(
        OnTimerContext context,
        @StateId("seqId") MapState<String, KV<Long, Long>> lastWriteSeqId,
        @StateId("sortBuffer") BagState<TimestampedValue<List<Internal>>> sortBuffer,
        @StateId("minBufferStamp") ValueState<Instant> minBufferStamp,
        @TimerId("sortTimer") Timer sortTimer,
        OutputReceiver<Internal> output) {

      Instant ts = context.fireTimestamp();
      final List<TimestampedValue<List<Internal>>> toFlush = new ArrayList<>();
      final List<TimestampedValue<List<Internal>>> toKeep = new ArrayList<>();
      Instant newMinStamp = BoundedWindow.TIMESTAMP_MAX_VALUE;
      for (TimestampedValue<List<Internal>> element : sortBuffer.read()) {
        if (ts.isAfter(element.getTimestamp())) {
          toFlush.add(element);
        } else {
          if (newMinStamp.isAfter(element.getTimestamp())) {
            newMinStamp = element.getTimestamp();
          }
          toKeep.add(element);
        }
      }
      if (newMinStamp.isBefore(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
        sortTimer.withOutputTimestamp(newMinStamp).set(newMinStamp.plus(1));
        minBufferStamp.write(newMinStamp);
      } else {
        minBufferStamp.clear();
      }
      sortBuffer.clear();
      toKeep.forEach(sortBuffer::add);
      toFlush
          .stream()
          .sorted(
              Comparator.<TimestampedValue<List<Internal>>, Instant>comparing(
                      TimestampedValue::getTimestamp)
                  .thenComparing(el -> el.getValue().get(0).getSeqId()))
          .map(TimestampedValue::getValue)
          .forEachOrdered(
              list -> processOrderedElement(list, context.timestamp(), lastWriteSeqId, output));
      long cleanupStamp = context.fireTimestamp().getMillis() - cleanupIntervalMs;
      for (Map.Entry<String, KV<Long, Long>> e : lastWriteSeqId.entries().read()) {
        if (e.getValue().getValue() < cleanupStamp) {
          lastWriteSeqId.remove(e.getKey());
        }
      }
    }

    private void processOrderedElement(
        List<Internal> element,
        Instant timestamp,
        MapState<String, KV<Long, Long>> lastWriteSeqId,
        OutputReceiver<Internal> output) {

      List<Internal> actions = Objects.requireNonNull(element);
      Internal commit = getCommitIfPresent(actions);
      if (commit != null) {
        if (verifyNoConflict(actions, commit.getSeqId(), timestamp, lastWriteSeqId)) {
          flushWrites(actions, commit.getSeqId(), timestamp, lastWriteSeqId);
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
        List<Internal> actions,
        long seqId,
        Instant timestamp,
        MapState<String, KV<Long, Long>> lastWriteSeqId) {

      actions
          .stream()
          .filter(a -> a.getRequest().getType().equals(Type.WRITE))
          .flatMap(a -> a.getKeyValueList().stream())
          .distinct()
          .forEach(kv -> lastWriteSeqId.put(kv.getKey(), KV.of(seqId, timestamp.getMillis())));
    }

    private boolean verifyNoConflict(
        List<Internal> actions,
        long commitSeqId,
        Instant actionStamp,
        MapState<String, KV<Long, Long>> lastWriteSeqId) {

      // caching
      actions
          .stream()
          .flatMap(a -> a.getKeyValueList().stream().map(KeyValue::getKey))
          .distinct()
          .forEach(k -> lastWriteSeqId.get(k).readLater());
      return actions
          .stream()
          .allMatch(a -> isValidRead(a, commitSeqId, actionStamp, lastWriteSeqId));
    }

    private boolean isValidRead(
        Internal action,
        long actionSeqId,
        Instant actionStamp,
        MapState<String, KV<Long, Long>> lastWriteSeqId) {

      return action
          .getKeyValueList()
          .stream()
          .allMatch(kv -> isValidKvAccess(actionSeqId, actionStamp, lastWriteSeqId, kv));
    }

    private boolean isValidKvAccess(
        long actionSeqId,
        Instant actionStamp,
        MapState<String, KV<Long, Long>> lastWriteSeqId,
        KeyValue kv) {

      boolean isRead = kv.getSeqId() != 0;
      @Nullable KV<Long, Long> lastWrite = lastWriteSeqId.get(kv.getKey()).read();
      if (lastWrite == null) {
        return true;
      }
      if (isRead) {
        return kv.getSeqId() == lastWrite.getKey();
      }
      return actionSeqId > lastWrite.getKey() && actionStamp.getMillis() > lastWrite.getValue();
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
    TransactionRunnerOptions opts =
        input.getPipeline().getOptions().as(TransactionRunnerOptions.class);
    int cleanupInterval = opts.getTransactionCleanupIntervalSeconds();
    return input
        .apply(WithKeys.of(Internal::getTransactionId).withKeyType(TypeDescriptors.strings()))
        .apply("gather", ParDo.of(new GatherTransactionRequestsFn()))
        .apply(WithKeys.of((Void) null))
        .apply("verify", ParDo.of(new VerifyTransactionsFn(cleanupInterval * 1000L)));
  }
}
