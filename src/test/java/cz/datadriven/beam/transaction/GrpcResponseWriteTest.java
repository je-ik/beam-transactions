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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.protobuf.TextFormat;
import cz.datadriven.beam.transaction.proto.InternalOuterClass.Internal;
import cz.datadriven.beam.transaction.proto.Server.Request;
import cz.datadriven.beam.transaction.proto.Server.Response;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Slf4j
public class GrpcResponseWriteTest {

  String testUuid;

  @BeforeEach
  void setup() {
    testUuid = UUID.randomUUID().toString();
    TestUtils.startTest(testUuid);
  }

  @Test
  @Timeout(15)
  void testResponse() throws ExecutionException, InterruptedException, TimeoutException {
    Pipeline p = Pipeline.create();
    p.apply(GrpcRequestRead.of(TestUtils.getMaxRequestsFn(testUuid, 1)))
        .apply(
            MapElements.into(
                    TypeDescriptors.kvs(
                        TypeDescriptors.strings(), TypeDescriptor.of(Internal.class)))
                .via(
                    request -> {
                      log.info("Have request {}", TextFormat.shortDebugString(request));
                      return KV.of(
                          request.getRequest().getResponseHost()
                              + ":"
                              + request.getRequest().getResponsePort(),
                          request);
                    }))
        .apply(GrpcResponseWrite.of());
    CompletableFuture<PipelineResult> future = CompletableFuture.supplyAsync(p::run);
    TimeUnit.SECONDS.sleep(3);
    try (TransactionClient client = TransactionClient.of("localhost", 5222)) {
      Response response = client.sendSync(Request.getDefaultInstance(), 10, TimeUnit.SECONDS);
      assertNotNull(response);
    }
    assertEquals(State.DONE, future.get().getState());
  }
}
