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

import java.io.Closeable;
import java.io.Serializable;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import lombok.Builder;
import org.apache.beam.sdk.values.KV;

public interface DatabaseAccessor extends Serializable, Closeable {

  @Builder
  @lombok.Value
  class Value {
    double amount;
    long seqId;
  }

  void setup();

  @Override
  void close();

  default void setBatch(Stream<KV<String, Value>> values) {
    values.forEach(v -> set(v.getKey(), v.getValue()));
  }

  default Stream<KV<String, Value>> getBatch(Stream<String> keys) {
    return keys.map(k -> KV.of(k, get(k)));
  }

  void set(String key, Value value);

  @Nullable
  Value get(String key);
}
