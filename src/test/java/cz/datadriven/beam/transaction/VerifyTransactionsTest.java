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

import cz.datadriven.beam.transaction.VerifyTransactions.GatherTransactionRequestsFn;
import cz.datadriven.beam.transaction.proto.InternalOuterClass.Internal;
import cz.datadriven.beam.transaction.proto.Server.KeyValue;
import cz.datadriven.beam.transaction.proto.Server.ReadPayload;
import cz.datadriven.beam.transaction.proto.Server.Request;
import cz.datadriven.beam.transaction.proto.Server.Request.Type;
import cz.datadriven.beam.transaction.proto.Server.WritePayload;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Instant;
import org.junit.jupiter.api.Test;

public class VerifyTransactionsTest {

  @Test
  void testGatherTransactionRequestsFn() {
    Internal read =
        asInternal(
            "t1",
            1L,
            Request.newBuilder()
                .setType(Type.READ)
                .setReadPayload(ReadPayload.newBuilder().addKey("key"))
                .build());
    Internal write =
        asInternal(
            "t1",
            1L,
            Request.newBuilder()
                .setType(Type.COMMIT)
                .setWritePayload(
                    WritePayload.newBuilder()
                        .addKeyValue(KeyValue.newBuilder().setKey("key").setValue(1.0)))
                .build());
    Instant now = new Instant(0);
    TestStream<Internal> input =
        TestStream.create(ProtoCoder.of(Internal.getDefaultInstance()))
            .addElements(TimestampedValue.of(read, now))
            .advanceWatermarkTo(now)
            .addElements(TimestampedValue.of(write, now.plus(1)))
            .advanceWatermarkToInfinity();
    Pipeline p = Pipeline.create();
    PCollection<String> result =
        p.apply(input)
            .apply(WithKeys.of(Internal::getTransactionId).withKeyType(TypeDescriptors.strings()))
            .apply(ParDo.of(new GatherTransactionRequestsFn()))
            .apply(
                MapElements.into(TypeDescriptors.strings())
                    .via(
                        e ->
                            e.stream()
                                .map(i -> i.getRequest().getType())
                                .sorted(Comparator.comparing(Type::getNumber))
                                .collect(Collectors.toList())
                                .toString()));
    PAssert.that(result).containsInAnyOrder("[READ]", "[READ, COMMIT]");
    p.run();
  }

  @Test
  public void testOkCommit() {
    Internal read =
        asInternal(
            "t1",
            2L,
            Request.newBuilder()
                .setType(Type.READ)
                .setReadPayload(ReadPayload.newBuilder().addKey("key"))
                .build(),
            Collections.singletonList(
                Internal.KeyValue.newBuilder().setKey("key").setValue(1.0).setSeqId(-1L).build()));
    Internal write =
        asInternal(
            "t1",
            2L,
            Request.newBuilder()
                .setType(Type.COMMIT)
                .setWritePayload(
                    WritePayload.newBuilder()
                        .addKeyValue(KeyValue.newBuilder().setKey("key").setValue(1.0)))
                .build(),
            Collections.singletonList(
                Internal.KeyValue.newBuilder().setKey("key").setValue(2.0).build()));
    Instant now = new Instant(0);
    TestStream<Internal> input =
        TestStream.create(ProtoCoder.of(Internal.getDefaultInstance()))
            .addElements(TimestampedValue.of(read, now))
            .advanceWatermarkTo(now)
            .addElements(TimestampedValue.of(write, now.plus(1)))
            .advanceWatermarkToInfinity();
    Pipeline p = Pipeline.create();
    PCollection<String> res =
        p.apply(input)
            .apply(new VerifyTransactions())
            .apply(
                MapElements.into(TypeDescriptors.strings())
                    .via(
                        a ->
                            String.format(
                                "%s:%d:%d",
                                a.getRequest().getType(), a.getSeqId(), a.getStatus())));
    PAssert.that(res).containsInAnyOrder("COMMIT:2:200");
    p.run();
  }

  @Test
  public void testFailedCommit() {
    Internal read =
        asInternal(
            "t1",
            2L,
            Request.newBuilder()
                .setType(Type.READ)
                .setReadPayload(ReadPayload.newBuilder().addKey("key"))
                .build(),
            Collections.singletonList(
                Internal.KeyValue.newBuilder().setKey("key").setValue(1.0).setSeqId(-1L).build()));
    Internal read2 =
        asInternal(
            "t2",
            3L,
            Request.newBuilder()
                .setType(Type.READ)
                .setReadPayload(ReadPayload.newBuilder().addKey("key"))
                .build(),
            Collections.singletonList(
                Internal.KeyValue.newBuilder().setKey("key").setValue(1.0).setSeqId(-1L).build()));
    Internal write =
        asInternal(
            "t1",
            2L,
            Request.newBuilder()
                .setType(Type.COMMIT)
                .setWritePayload(
                    WritePayload.newBuilder()
                        .addKeyValue(KeyValue.newBuilder().setKey("key").setValue(1.0)))
                .build(),
            Collections.singletonList(
                Internal.KeyValue.newBuilder().setKey("key").setValue(2.0).build()));
    Internal commit2 = asInternal("t2", 3L, Request.newBuilder().setType(Type.COMMIT).build());
    Instant now = new Instant(0);
    TestStream<Internal> input =
        TestStream.create(ProtoCoder.of(Internal.getDefaultInstance()))
            .addElements(TimestampedValue.of(read, now), TimestampedValue.of(read2, now))
            .advanceWatermarkTo(now)
            .addElements(TimestampedValue.of(write, now.plus(1)))
            .advanceWatermarkTo(now.plus(1))
            .addElements(TimestampedValue.of(commit2, now.plus(3)))
            .advanceWatermarkToInfinity();
    Pipeline p = Pipeline.create();
    PCollection<String> res =
        p.apply(input)
            .apply(new VerifyTransactions())
            .apply(
                MapElements.into(TypeDescriptors.strings())
                    .via(
                        a ->
                            String.format(
                                "%s:%d:%d",
                                a.getRequest().getType(), a.getSeqId(), a.getStatus())));
    PAssert.that(res).containsInAnyOrder("COMMIT:2:200", "COMMIT:3:412");
    p.run();
  }

  private Internal asInternal(String transactionId, long seqId, Request request) {
    return asInternal(transactionId, seqId, request, null);
  }

  private Internal asInternal(
      String transactionId,
      long seqId,
      Request request,
      @Nullable List<Internal.KeyValue> keyValues) {
    return Internal.newBuilder()
        .addAllKeyValue(keyValues == null ? Collections.emptyList() : keyValues)
        .setRequest(request.toBuilder().setTransactionId(transactionId))
        .setSeqId(seqId)
        .setTransactionId(transactionId)
        .build();
  }
}
