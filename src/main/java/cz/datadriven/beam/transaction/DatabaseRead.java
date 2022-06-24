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

import cz.datadriven.beam.transaction.DatabaseAccessor.Value;
import cz.datadriven.beam.transaction.proto.InternalOuterClass.Internal;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

public class DatabaseRead extends PTransform<PCollection<Internal>, PCollection<Internal>> {

  public static DatabaseRead of(DatabaseAccessor accessor) {
    return new DatabaseRead(accessor);
  }

  private static class DatabaseReadFn extends DoFn<KV<Integer, Iterable<Internal>>, Internal> {

    private final DatabaseAccessor accessor;

    public DatabaseReadFn(DatabaseAccessor accessor) {
      this.accessor = accessor;
    }

    @Setup
    public void setup() {
      accessor.setup();
    }

    @Teardown
    public void teardown() {
      accessor.close();
    }

    @ProcessElement
    public void process(
        @Element KV<Integer, Iterable<Internal>> element, OutputReceiver<Internal> output) {

      Iterable<Internal> requests = Objects.requireNonNull(element.getValue());
      Map<Long, List<String>> grouped =
          StreamSupport.stream(requests.spliterator(), false)
              .flatMap(r -> r.getKeyValueList().stream().map(k -> KV.of(r.getSeqId(), k.getKey())))
              .collect(
                  Collectors.groupingBy(
                      KV::getKey, Collectors.mapping(KV::getValue, Collectors.toList())));
      Map<String, Value> resolved =
          accessor
              .getBatch(
                  StreamSupport.stream(requests.spliterator(), false)
                      .flatMap(r -> r.getKeyValueList().stream().map(Internal.KeyValue::getKey))
                      .distinct())
              .collect(
                  Collectors.toMap(
                      KV::getKey,
                      kv -> kv.getValue() != null ? kv.getValue() : new Value(0.0, -1L)));
      for (Internal r : requests) {
        Internal.Builder builder = r.toBuilder().clearKeyValue();
        List<String> query = Objects.requireNonNull(grouped.get(r.getSeqId()));
        query.forEach(
            q -> {
              Value value = resolved.get(q);
              builder.addKeyValue(
                  Internal.KeyValue.newBuilder()
                      .setKey(q)
                      .setValue(value.getAmount())
                      .setSeqId(value.getSeqId()));
            });
        output.output(builder.build());
      }
    }
  }

  private final DatabaseAccessor accessor;

  public DatabaseRead(DatabaseAccessor accessor) {
    this.accessor = accessor;
  }

  @Override
  public PCollection<Internal> expand(PCollection<Internal> input) {
    return input
        .apply(
            MapElements.into(
                    TypeDescriptors.kvs(
                        TypeDescriptors.integers(), TypeDescriptor.of(Internal.class)))
                .via(e -> KV.of(e.getTransactionId().hashCode() % 50, e)))
        .apply(
            GroupIntoBatches.<Integer, Internal>ofSize(50)
                .withMaxBufferingDuration(Duration.millis(20)))
        .apply(ParDo.of(new DatabaseReadFn(accessor)));
  }
}
