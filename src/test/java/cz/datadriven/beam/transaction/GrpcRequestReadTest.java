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

import static org.junit.jupiter.api.Assertions.assertTrue;

import cz.datadriven.beam.transaction.proto.InternalOuterClass.Internal;
import cz.datadriven.beam.transaction.proto.Server.Request;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Slf4j
public class GrpcRequestReadTest {

  String testUuid;

  @BeforeEach
  void setup() {
    testUuid = UUID.randomUUID().toString();
    TestUtils.startTest(testUuid);
  }

  @Test
  @Timeout(15)
  void testRun() throws ExecutionException, InterruptedException {
    Pipeline p = Pipeline.create();
    TransactionRunner.registerCoders(p);
    int numRequests = 10;
    SerializableFunction<Request, Instant> watermarkFn =
        TestUtils.getMaxRequestsFn(testUuid, numRequests);
    PCollection<Internal> requests = p.apply(GrpcRequestRead.of(watermarkFn));
    PCollection<Long> result =
        requests
            .apply(
                Window.<Internal>into(new GlobalWindows())
                    .triggering(AfterWatermark.pastEndOfWindow())
                    .discardingFiredPanes())
            .apply(Combine.globally(Count.<Internal>combineFn()).withoutDefaults());
    PAssert.that(result).containsInAnyOrder((long) numRequests);
    CompletableFuture<Boolean> written =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                TimeUnit.SECONDS.sleep(3);
                try (TransactionClient client = TransactionClient.of("localhost", 5222)) {
                  for (int i = 0; i < numRequests; i++) {
                    client.sendRequestAsync(Request.getDefaultInstance());
                  }
                  return true;
                }
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    try {
      p.run();
    } catch (Exception ex) {
      ex.printStackTrace(System.err);
      throw ex;
    }
    assertTrue(written.get());
  }
}
