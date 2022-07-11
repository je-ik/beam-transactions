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
import cz.datadriven.beam.transaction.DatabaseAccessor.Value;
import cz.datadriven.beam.transaction.proto.InternalOuterClass.Internal;
import cz.datadriven.beam.transaction.proto.InternalOuterClass.Internal.KeyValue;
import cz.datadriven.beam.transaction.proto.Server.Request.Type;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Instant;

public class DatabaseWrite extends PTransform<PCollection<Internal>, PDone> {

  public static DatabaseWrite of(DatabaseAccessor accessor) {
    return new DatabaseWrite(accessor);
  }

  private final DatabaseAccessor accessor;

  public DatabaseWrite(DatabaseAccessor accessor) {
    this.accessor = accessor;
  }

  @Override
  public PDone expand(PCollection<Internal> input) {
    input
        .apply(Filter.by(a -> a.getRequest().getType().equals(Type.COMMIT)))
        .apply(
            FlatMapElements.into(
                    TypeDescriptors.kvs(
                        TypeDescriptors.strings(), TypeDescriptor.of(KeyValue.class)))
                .via(
                    a ->
                        a.getKeyValueList()
                            .stream()
                            .map(
                                kv ->
                                    KV.of(
                                        kv.getKey(), kv.toBuilder().setSeqId(a.getSeqId()).build()))
                            .collect(Collectors.toList())))
        .apply(ParDo.of(new WriteFn(accessor)));
    return PDone.in(input.getPipeline());
  }

  private static class WriteFn extends DoFn<KV<String, KeyValue>, Void> {

    private final DatabaseAccessor accessor;

    @StateId("lastWritten")
    private final StateSpec<ValueState<Long>> lastWrittenSpec = StateSpecs.value();

    public WriteFn(DatabaseAccessor accessor) {
      this.accessor = accessor;
    }

    @Setup
    public void setup() {
      accessor.setup();
    }

    @Teardown
    public void tearDown() {
      accessor.close();
    }

    @ProcessElement
    public void process(
        @Element KV<String, KeyValue> element,
        @Timestamp Instant ts,
        @StateId("lastWritten") ValueState<Long> lastWritten) {

      long written = MoreObjects.firstNonNull(lastWritten.read(), 0L);
      KeyValue value = Objects.requireNonNull(element.getValue());
      if (written < value.getSeqId()) {
        accessor.set(value.getKey(), new Value(value.getValue(), value.getSeqId(), ts.getMillis()));
        lastWritten.write(value.getSeqId());
      }
    }
  }
}
