package com.inmobi.messaging.util;

import com.inmobi.messaging.ClientConfig;
import com.inmobi.databus.audit.AuditStats;
import com.inmobi.databus.audit.LatencyColumns;
import com.inmobi.messaging.consumer.audit.Tier;
import com.inmobi.databus.audit.Tuple;
import junit.framework.Assert;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

public class AuditDBUtil {
  protected Connection connection;
  protected Tuple tuple1, tuple2, tuple3, tuple4;
  protected Set<Tuple> tupleSet1, tupleSet2, tupleSet3;
  protected Date fromDate = new Date(1355314200000l);
  protected Date toDate = new Date(1355314400000l);

  public void setupDB(boolean updateDB) {
    ClientConfig config = ClientConfig.loadFromClasspath(AuditStats.CONF_FILE);
    connection = AuditDBHelper.getConnection(
        config.getString(AuditDBConstants.JDBC_DRIVER_CLASS_NAME),
        config.getString(AuditDBConstants.DB_URL),
        config.getString(AuditDBConstants.DB_USERNAME),
        config.getString(AuditDBConstants.DB_PASSWORD));
    Assert.assertTrue(connection != null);
    String dropTable = "DROP TABLE IF EXISTS "+AuditDBConstants
        .TABLE_NAME.toUpperCase()+";";
    String createTable =
        "CREATE TABLE audit(\n  TIMEINTERVAL bigint,\n  HOSTNAME varchar(25)," +
            "\n  TIER varchar(15),\n  TOPIC varchar(25)," +
            "\n  CLUSTER varchar(50),\n  SENT bigint,\n  C0 bigint," +
            "\n  C1 bigint,\n  C2 bigint,\n  C3 bigint,\n  C4 bigint," +
            "\n  C5 bigint,\n  C6 bigint,\n  C7 bigint,\n  C8 bigint," +
            "\n  C9 bigint,\n  C10 bigint,\n  C15 bigint,\n  C30 bigint," +
            "\n  C60 bigint,\n  C120 bigint,\n  C240 bigint,\n  C600 bigint,\n" +
            "  PRIMARY KEY (TIMEINTERVAL,HOSTNAME,TIER,TOPIC,CLUSTER)\n)";
    try {
      connection.prepareStatement(dropTable).execute();
      connection.prepareStatement(createTable).execute();
    } catch (SQLException e) {
      e.printStackTrace();
    }
    createTuples();
    if (updateDB) {
      updateDBWithData();
    }
  }

  private void updateDBWithData() {
    ClientConfig config = ClientConfig.loadFromClasspath(AuditStats.CONF_FILE);
    AuditDBHelper dbHelper = new AuditDBHelper(config);
    boolean isSuccessful = dbHelper.update(tupleSet1);
    Assert.assertTrue(isSuccessful);
    isSuccessful = dbHelper.update(tupleSet2);
    Assert.assertTrue(isSuccessful);
    isSuccessful = dbHelper.update(tupleSet3);
    Assert.assertTrue(isSuccessful);
  }

  private void createTuples() {
    /*
     Add tuples in tuplesets so that insert, update are possible
     */
    String hostname1 = "testhost1";
    String hostname2 = "testhost2";
    String tier = Tier.AGENT.toString();
    String cluster = "testCluster";
    Date timestamp = new Date(1355314332000l);
    String topic = "testTopic";
    String topic2 = "testTopic1";
    Map<LatencyColumns, Long> latencyCountMap1 =
        new HashMap<LatencyColumns, Long>();
    Map<LatencyColumns, Long> latencyCountMap2 =
        new HashMap<LatencyColumns, Long>();
    Map<LatencyColumns, Long> latencyCountMap3 =
        new HashMap<LatencyColumns, Long>();
    latencyCountMap1.put(LatencyColumns.C1, 500l);
    latencyCountMap1.put(LatencyColumns.C0, 1500l);
    latencyCountMap2.put(LatencyColumns.C1, 1000l);
    latencyCountMap2.put(LatencyColumns.C2, 1000l);
    latencyCountMap2.put(LatencyColumns.C3, 500l);
    latencyCountMap3.put(LatencyColumns.C600, 1000l);
    Long sent1 = 2000l;
    Long sent2 = 2500l;
    Long sent3 = 1000l;

    tuple1 =
        new Tuple(hostname1, tier, cluster, timestamp, topic, latencyCountMap1,
            sent1);
    tuple2 =
        new Tuple(hostname1, tier, cluster, timestamp, topic, latencyCountMap2,
            sent2);
    tuple3 =
        new Tuple(hostname1, tier, cluster, timestamp, topic2, latencyCountMap3,
            sent3);
    tuple4 =
        new Tuple(hostname2, tier, cluster, timestamp, topic, latencyCountMap1,
            sent1);

    tupleSet1 = new HashSet<Tuple>();
    tupleSet2 = new HashSet<Tuple>();
    tupleSet3 = new HashSet<Tuple>();
    tupleSet1.add(tuple1);
    tupleSet2.add(tuple2);
    tupleSet3.add(tuple3);
    tupleSet3.add(tuple4);
  }

  public void shutDown() {
    try {
      connection.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
