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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;

public class Utils {

  private static Map<String, Integer> testContext = new ConcurrentHashMap<>();

  static void startTest(String testUid) {
    testContext.put(testUid, 0);
  }

  static SerializableFunction<Request, Instant> getMaxRequestsFn(String testUuid, int numRequests) {
    return req -> {
      int current = testContext.compute(testUuid, (k, v) -> v + 1);
      if (current >= numRequests) {
        return BoundedWindow.TIMESTAMP_MAX_VALUE;
      }
      return Instant.now();
    };
  }

  private Utils() {}
}
