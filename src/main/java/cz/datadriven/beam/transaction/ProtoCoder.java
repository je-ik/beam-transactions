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

import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Message.Builder;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;

public class ProtoCoder<M extends GeneratedMessageV3> extends CustomCoder<M> {

  public static <M extends GeneratedMessageV3> Coder<M> of(M defaultInstance) {
    return new ProtoCoder<>(defaultInstance);
  }

  private final M defaultInstance;

  ProtoCoder(M defaultInstance) {
    this.defaultInstance = defaultInstance;
  }

  @Override
  public void encode(M value, OutputStream outStream) throws IOException {
    value.writeDelimitedTo(outStream);
  }

  @SuppressWarnings("unchecked")
  @Override
  public M decode(InputStream inStream) throws IOException {
    Builder builder = defaultInstance.toBuilder();
    builder.mergeDelimitedFrom(inStream);
    return (M) builder.build();
  }
}
