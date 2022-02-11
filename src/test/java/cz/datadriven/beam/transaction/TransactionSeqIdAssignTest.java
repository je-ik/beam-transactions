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

import cz.datadriven.beam.transaction.proto.Server.Request;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.jupiter.api.Test;

public class TransactionSeqIdAssignTest {

  @Test
  public void testAssignment() {
    Pipeline p = Pipeline.create();
    PCollection<Request> requests =
        p.apply(
            Create.of(
                Request.getDefaultInstance(),
                Request.getDefaultInstance(),
                Request.getDefaultInstance()));
    PCollection<Long> ids =
        requests
            .apply(TransactionSeqIdAssign.of())
            .apply(MapElements.into(TypeDescriptors.longs()).via(Request::getSeqId));
    PAssert.that(ids).containsInAnyOrder(1L, 2L, 3L);
    p.run();
  }
}
