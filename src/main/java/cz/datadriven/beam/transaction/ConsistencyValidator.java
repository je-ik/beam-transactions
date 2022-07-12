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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.base.Preconditions;
import java.net.InetSocketAddress;

public class ConsistencyValidator {

  public static void main(String[] args) {
    if (args.length < 1) {
      usage();
    }
    String authority = args[0];
    String[] parts = authority.split(":");
    Preconditions.checkArgument(
        parts.length == 2, "Authority must be a host:port, got %s", authority);
    try (Cluster cluster =
        Cluster.builder()
            .addContactPointsWithPorts(
                InetSocketAddress.createUnresolved(parts[0], Integer.parseInt(parts[1])))
            .build()) {
      verifyWithCluster(cluster);
    }
  }

  private static void verifyWithCluster(Cluster cluster) {
    double sum = 0.0;
    long rows = 0;
    long maxSeqId = 0L;
    try (Session session = cluster.newSession()) {
      ResultSet result = session.execute("SELECT amount, seqId FROM beam.amounts");
      for (Row r : result) {
        sum += r.getDouble(0);
        rows++;
        long seqId = r.getLong(1);
        if (maxSeqId < seqId) {
          maxSeqId = seqId;
        }
      }
    }
    System.out.printf(
        "Sum of all amounts equals to: %f, rows: %d, maxSeqId: %d\n", sum, rows, maxSeqId);
  }

  private static void usage() {
    System.err.printf(
        "Usage: %s <cassandra_authority>\n", ConsistencyValidator.class.getSimpleName());
    System.exit(1);
  }
}
