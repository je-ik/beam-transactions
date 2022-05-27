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

import static org.junit.jupiter.api.Assertions.assertEquals;

import cz.datadriven.beam.transaction.proto.InternalOuterClass.Internal;
import cz.datadriven.beam.transaction.proto.InternalOuterClass.Internal.KeyValue;
import cz.datadriven.beam.transaction.proto.Server.Request;
import cz.datadriven.beam.transaction.proto.Server.Request.Type;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.junit.jupiter.api.Test;

public class DatabaseWriteTest {

  @Test
  void testDatabaseWrite() {
    DatabaseAccessor accessor = new MemoryDatabaseAccessor();
    Internal write1 = write("key", 1.0, 1L);
    Internal write2 = write("key2", 2.0, 2L);
    Pipeline p = Pipeline.create();
    p.apply(Create.of(write1, write2)).apply(DatabaseWrite.of(accessor));
    p.run().waitUntilFinish();
    assertEquals(1.0, accessor.get("key").getAmount(), 0.0001);
    assertEquals(1L, accessor.get("key").getSeqId());
    assertEquals(2.0, accessor.get("key2").getAmount(), 0.0001);
    assertEquals(2L, accessor.get("key2").getSeqId());
  }

  private Internal write(String key, double value, long seqId) {
    return Internal.newBuilder()
        .setRequest(Request.newBuilder().setType(Type.WRITE))
        .addKeyValue(KeyValue.newBuilder().setKey(key).setValue(value).setSeqId(seqId))
        .build();
  }
}
