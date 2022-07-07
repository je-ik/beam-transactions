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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Create keyspace and table:
 *
 * <pre>
 * cqlsh> CREATE KEYSPACE beam WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 3};
 * cqlsh> CREATE TABLE beam.amounts (id varchar PRIMARY KEY, amount double, seqId bigint);
 * </pre>
 */
public class CassandraDatabaseAccessor implements DatabaseAccessor {

  private final List<InetSocketAddress> contactPoints;
  private final String keyspace;
  private final String table;
  private Cluster cluster;
  private Session session;
  private PreparedStatement readStatement;
  private PreparedStatement writeStatement;

  public CassandraDatabaseAccessor(String authority, String keyspace, String table) {
    Preconditions.checkArgument(!authority.isBlank(), "Missing cassandra contact point");
    this.contactPoints = getContactPoints(authority);
    this.keyspace = keyspace;
    this.table = table;
  }

  private List<InetSocketAddress> getContactPoints(String authority) {
    return Arrays.stream(authority.split(","))
        .map(
            point -> {
              String[] parts = point.split(":");
              Preconditions.checkArgument(parts.length == 2, "Invalid contact point %s", point);
              return InetSocketAddress.createUnresolved(parts[0], Integer.parseInt(parts[1]));
            })
        .collect(Collectors.toList());
  }

  @Override
  public void setup() {
    cluster = Cluster.builder().addContactPointsWithPorts(contactPoints).build();
  }

  @Override
  public void close() {
    cluster.close();
  }

  @Override
  public void set(String key, Value value) {
    ensureSession();
    BoundStatement statement = writeStatement.bind(value.getAmount(), value.getSeqId(), key);
    session.execute(statement);
  }

  @Nullable
  @Override
  public Value get(String key) {
    ensureSession();
    ResultSet result = session.execute(readStatement.bind(key));
    List<Row> rows = result.all();
    if (rows.isEmpty()) {
      return null;
    }
    Row row = Iterables.getOnlyElement(rows);
    return new Value(row.get(0, Double.class), row.get(1, Long.class));
  }

  private void ensureSession() {
    if (session == null || session.isClosed()) {
      session = cluster.newSession();
      readStatement =
          session.prepare(
              String.format("SELECT amount, seqId FROM %s.%s WHERE id = ?", keyspace, table));
      writeStatement =
          session.prepare(
              String.format(
                  "INSERT INTO %s.%s (amount, seqId, id) VALUES (?, ?, ?)", keyspace, table));
    }
  }
}
