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

import com.google.common.annotations.VisibleForTesting;
import cz.datadriven.beam.transaction.proto.InternalOuterClass.Internal;
import cz.datadriven.beam.transaction.proto.Server.Request;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;

public class GrpcRequestRead extends PTransform<PBegin, PCollection<Internal>> {;

  public static GrpcRequestRead of() {
    return new GrpcRequestRead(null);
  }

  @VisibleForTesting
  static GrpcRequestRead of(SerializableFunction<Request, Instant> watemarkFn) {
    return new GrpcRequestRead(watemarkFn);
  }

  private final SerializableFunction<Request, Instant> watermarkFn;

  public GrpcRequestRead(SerializableFunction<Request, Instant> watemarkFn) {
    this.watermarkFn = watemarkFn;
  }

  @Override
  public PCollection<Internal> expand(PBegin input) {
    return input
        .apply(Impulse.create())
        .apply(
            ParDo.of(
                new GrpcRequestReadFn(
                    input.getPipeline().getOptions().as(TransactionRunnerOptions.class),
                    watermarkFn)));
  }
}
