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
import cz.datadriven.beam.transaction.proto.InternalOuterClass.Internal;
import cz.datadriven.beam.transaction.proto.Server.Request.Type;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

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
        .apply(Filter.by(a -> a.getRequest().getType().equals(Type.WRITE)))
        .apply(Reshuffle.viaRandomKey())
        .apply(ParDo.of(new WriteFn(accessor)));
    return PDone.in(input.getPipeline());
  }

  private static class WriteFn extends DoFn<Internal, Void> {

    private final DatabaseAccessor accessor;

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

    @RequiresStableInput
    @ProcessElement
    public void process(@Element Internal element) {
      accessor.setBatch(
          element
              .getKeyValueList()
              .stream()
              .map(kv -> KV.of(kv.getKey(), new Value(kv.getValue(), kv.getSeqId()))));
    }
  }
}
