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

import cz.datadriven.beam.transaction.DatabaseAccessor.Value;
import cz.datadriven.beam.transaction.proto.Server.KeyValue;
import cz.datadriven.beam.transaction.proto.Server.Request;
import cz.datadriven.beam.transaction.proto.Server.Response;
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

public class DatabaseRead extends PTransform<PCollection<Request>, PCollection<Response>> {

  public static DatabaseRead of(DatabaseAccessor accessor) {
    return new DatabaseRead(accessor);
  }

  private static class DatabaseReadFn extends DoFn<KV<Integer, Iterable<Request>>, Response> {

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
        @Element KV<Integer, Iterable<Request>> element, OutputReceiver<Response> output) {
      Iterable<Request> requests = element.getValue();
      Map<String, List<String>> grouped =
          StreamSupport.stream(requests.spliterator(), false)
              .flatMap(
                  r ->
                      r.getReadPayload()
                          .getKeyList()
                          .stream()
                          .map(k -> KV.of(r.getRequestUid(), k)))
              .collect(
                  Collectors.groupingBy(
                      KV::getKey, Collectors.mapping(KV::getValue, Collectors.toList())));
      Map<String, Value> resolved =
          accessor
              .getBatch(
                  StreamSupport.stream(requests.spliterator(), false)
                      .flatMap(r -> r.getReadPayload().getKeyList().stream())
                      .distinct())
              .collect(
                  Collectors.toMap(
                      KV::getKey,
                      kv -> kv.getValue() != null ? kv.getValue() : new Value(0.0, 0L)));
      for (Request r : requests) {
        Response.Builder builder = Response.newBuilder().setRequestUid(r.getRequestUid());
        List<String> query = Objects.requireNonNull(grouped.get(r.getRequestUid()));
        query.forEach(
            q ->
                builder.addKeyvalue(
                    KeyValue.newBuilder().setKey(q).setValue(resolved.get(q).getAmount())));
        output.output(builder.build());
      }
    }
  }

  private final DatabaseAccessor accessor;

  public DatabaseRead(DatabaseAccessor accessor) {
    this.accessor = accessor;
  }

  @Override
  public PCollection<Response> expand(PCollection<Request> input) {
    return input
        .apply(
            MapElements.into(
                    TypeDescriptors.kvs(
                        TypeDescriptors.integers(), TypeDescriptor.of(Request.class)))
                .via(e -> KV.of(e.getTransactionId().hashCode() % 50, e)))
        .apply(
            GroupIntoBatches.<Integer, Request>ofSize(50)
                .withMaxBufferingDuration(Duration.millis(20)))
        .apply(ParDo.of(new DatabaseReadFn(accessor)));
  }
}
