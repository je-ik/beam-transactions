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

import cz.datadriven.beam.transaction.proto.Server.Request;
import cz.datadriven.beam.transaction.proto.Server.Request.Type;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;

public class TestUtils {

  private static final Map<String, Integer> TEST_CONTEXTS = new ConcurrentHashMap<>();

  static class AfterNCommits implements SerializableFunction<Request, Instant> {
    private static final AtomicInteger numCommits = new AtomicInteger();
    private final int n;

    AfterNCommits(int n) {
      this.n = n;
    }

    @Override
    public Instant apply(Request request) {
      if (request != null
          && request.getType().equals(Type.COMMIT)
          && numCommits.incrementAndGet() >= n) {

        return BoundedWindow.TIMESTAMP_MAX_VALUE;
      }
      Instant now = Instant.now();
      return now;
    }
  }

  static class StaticBoolean implements Serializable {

    private static volatile boolean VALUE;

    boolean get() {
      return VALUE;
    }

    void set() {
      VALUE = true;
    }
  }

  static void startTest(String testUid) {
    TEST_CONTEXTS.put(testUid, 0);
  }

  static SerializableFunction<Request, Instant> getMaxRequestsFn(String testUuid, int numRequests) {
    return req -> {
      if (req != null) {
        int current = TEST_CONTEXTS.compute(testUuid, (k, v) -> v + 1);
        if (current >= numRequests) {
          return BoundedWindow.TIMESTAMP_MAX_VALUE;
        }
      }
      return Instant.now();
    };
  }

  private TestUtils() {}
}
