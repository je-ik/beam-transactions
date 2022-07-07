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

import com.google.auto.service.AutoService;
import java.util.Collections;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsRegistrar;

public interface TransactionRunnerOptions extends PipelineOptions {

  @AutoService(PipelineOptionsRegistrar.class)
  class TransactionRunnerOptionsRegistrar implements PipelineOptionsRegistrar {
    @Override
    public Iterable<Class<? extends PipelineOptions>> getPipelineOptions() {
      return Collections.singletonList(TransactionRunnerOptions.class);
    }
  }

  @Default.Integer(5222)
  int getRequestPort();

  void setRequestPort(int port);

  @Default.Integer(4)
  int getSourceParallelism();

  void setSourceParallelism(int parallelism);

  String getCassandraAuthority();

  void setCassandraAuthority(String authority);

  @Default.String("beam")
  String getCassandraKeyspace();

  void setCassandraKeyspace(String keyspace);

  @Default.String("amounts")
  String getCassandraTable();

  void setCassandraTable(String table);

  @Default.Integer(50)
  int getGrpcReadDelay();

  void setGrpcReadDelay(int delay);

  @Default.Integer(1)
  int getNumInitialSplits();

  void setNumInitialSplits(int splits);
}
