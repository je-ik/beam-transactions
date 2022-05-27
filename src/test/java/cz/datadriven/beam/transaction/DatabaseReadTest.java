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

import com.google.common.collect.Iterables;
import cz.datadriven.beam.transaction.proto.InternalOuterClass.Internal;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.jupiter.api.Test;

public class DatabaseReadTest {

  @Test
  void testRead() {
    DatabaseAccessor accessor = new MemoryDatabaseAccessor();
    accessor.set("1", new DatabaseAccessor.Value(1.0, 1L));
    accessor.set("2", new DatabaseAccessor.Value(2.0, 2L));
    accessor.set("3", new DatabaseAccessor.Value(3.0, 3L));
    List<Internal> inputs = Arrays.asList(newRequest(1L, "1", "3"), newRequest(2L, "2", "4"));
    Pipeline pipeline = Pipeline.create();
    PCollection<String> result =
        pipeline
            .apply(Create.of(inputs))
            .apply(DatabaseRead.of(accessor))
            .apply(
                FlatMapElements.into(TypeDescriptors.strings())
                    .via(
                        e ->
                            Iterables.transform(
                                e.getKeyValueList(),
                                kv -> e.getSeqId() + ":" + kv.getKey() + "=" + kv.getValue())));

    PAssert.that(result).containsInAnyOrder("1:1=1.0", "1:3=3.0", "2:2=2.0", "2:4=0.0");

    pipeline.run();
  }

  private Internal newRequest(long seqId, String key, String... other) {
    Internal.Builder builder =
        Internal.newBuilder()
            .setSeqId(seqId)
            .addKeyValue(Internal.KeyValue.newBuilder().setKey(key));
    for (String k : other) {
      builder = builder.addKeyValue(Internal.KeyValue.newBuilder().setKey(k));
    }
    return builder.build();
  }
}
