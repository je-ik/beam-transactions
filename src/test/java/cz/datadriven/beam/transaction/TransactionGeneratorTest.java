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

import org.junit.jupiter.api.Test;

public class TransactionGeneratorTest {

  @Test
  void testMetrics() {
    TransactionGenerator app =
        new TransactionGenerator("localhost:1234", "localhost", 100, 10000, 10);
    for (int i = 0; i < 150; i++) {
      if (i % 3 != 0) {
        app.increaseCommitted();
      } else {
        app.increaseRejected();
      }
    }
    assertEquals(50, app.getRejectedSnapshot().getValues().length);
    assertEquals(100, app.getCommittedSnapshot().getValues().length);
  }
}
