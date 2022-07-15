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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;

public class MemoryDatabaseAccessor implements DatabaseAccessor {

  private static final Map<String, Map<String, Value>> DATA = new ConcurrentHashMap<>();

  private final String uuid = UUID.randomUUID().toString();

  public MemoryDatabaseAccessor() {
    DATA.put(uuid, new ConcurrentHashMap<>());
  }

  @Override
  public void setup() {
    // nop
  }

  @Override
  public void close() {
    // nop
  }

  @Override
  public void set(String key, Value value) {
    Preconditions.checkState(value.getStamp() > 0L);
    DATA.get(uuid)
        .compute(
            key,
            (k, v) -> {
              Preconditions.checkState(
                  v == null || v.getStamp() < value.getStamp(),
                  "Invalid overwrite of value %s, last %s",
                  value,
                  v);
              return value;
            });
  }

  @Nullable
  @Override
  public Value get(String key) {
    return DATA.get(uuid).get(key);
  }
}
