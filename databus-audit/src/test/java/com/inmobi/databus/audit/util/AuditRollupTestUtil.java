package com.inmobi.databus.audit.util;

import com.inmobi.databus.audit.LatencyColumns;
import com.inmobi.databus.audit.Tuple;
import com.inmobi.messaging.ClientConfig;
import junit.framework.Assert;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

public class AuditRollupTestUtil {

  Connection connection;
  protected ClientConfig config;
  protected Date currentDate;

  private Calendar calendar = Calendar.getInstance();

  public void setup() {

    config = ClientConfig.load("./src/test/resources/audit-rollup.properties");
    calendar.add(Calendar.DAY_OF_YEAR, -2);
    currentDate = calendar.getTime();

    try {
      String mainTable = config.getString(AuditDBConstants.MASTER_TABLE_NAME);
      connection = getConnection(config);
      createTable(mainTable);
      populateTuplesInDB();
    } finally {
      shutDown();
    }
  }

  private void createTable(String tableName) {
    String dropTable = "DROP TABLE IF EXISTS " + tableName.toUpperCase() + ";";
    String createTable =
        "CREATE TABLE "+ tableName.toUpperCase() + "(\nTIMEINTERVAL bigint," +
            "\n  HOSTNAME varchar(50)," +
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
  }

  public Connection getConnection(ClientConfig config) {
    Connection connection = AuditDBHelper.getConnection(
        config.getString(AuditDBConstants.JDBC_DRIVER_CLASS_NAME),
        config.getString(AuditDBConstants.DB_URL),
        config.getString(AuditDBConstants.DB_USERNAME),
        config.getString(AuditDBConstants.DB_PASSWORD));
    Assert.assertTrue(connection != null);
    return connection;
  }

  private void populateTuplesInDB() {
    String hostname = "testhost";
    String cluster = "testcluster";
    String topic = "testtopic";
    String tier = "agent";
    Set<Tuple> tupleSet = new HashSet<Tuple>();
    Map<LatencyColumns, Long> latencyMap = new HashMap<LatencyColumns, Long>();

    //set calendar to hour 1 of current day minute 5
    calendar.set(Calendar.HOUR_OF_DAY, 1);
    calendar.set(Calendar.MINUTE, 5);
    for (LatencyColumns latencyColumn : LatencyColumns.values()) {
      latencyMap.put(latencyColumn, 0l);
    }
    latencyMap.put(LatencyColumns.C0, 1500l);
    latencyMap.put(LatencyColumns.C1, 1500l);
    Tuple tuple = new Tuple(hostname, tier, cluster, calendar.getTime(),
        topic, latencyMap, 3000l);
    tupleSet.add(tuple);

    calendar.add(Calendar.MINUTE, 5);
    latencyMap.clear();
    for (LatencyColumns latencyColumn : LatencyColumns.values()) {
      latencyMap.put(latencyColumn, 0l);
    }
    latencyMap.put(LatencyColumns.C2, 1500l);
    latencyMap.put(LatencyColumns.C3, 1500l);
    tuple = new Tuple(hostname, tier, cluster, calendar.getTime(), topic,
        latencyMap, 3000l);
    tupleSet.add(tuple);

    calendar.add(Calendar.HOUR_OF_DAY, 1);
    for (LatencyColumns latencyColumn : LatencyColumns.values()) {
      latencyMap.put(latencyColumn, 0l);
    }
    latencyMap.put(LatencyColumns.C0, 1500l);
    latencyMap.put(LatencyColumns.C1, 1500l);
    tuple = new Tuple(hostname, tier, cluster, calendar.getTime(), topic,
        latencyMap, 3000l);
    tupleSet.add(tuple);

    AuditDBHelper helper = new AuditDBHelper(config);
    Assert.assertTrue(helper.update(tupleSet));
  }

  public void shutDown() {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }
}
