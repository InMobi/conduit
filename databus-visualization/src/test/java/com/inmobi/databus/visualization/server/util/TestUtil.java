package com.inmobi.databus.visualization.server.util;

import com.inmobi.messaging.ClientConfig;
import junit.framework.Assert;

import java.sql.Connection;
import java.sql.SQLException;

public class TestUtil {
  public static final String

  public static Connection setupDB() {
    //setupDb with some basic tuples
    Connection connection;
    ClientConfig config = ClientConfig.loadFromClasspath(CONF_FILE);
    connection = AuditDBHelper.getConnection(
        config.getString(AuditDBConstants.JDBC_DRIVER_CLASS_NAME),
        config.getString(AuditDBConstants.DB_URL),
        config.getString(AuditDBConstants.DB_USERNAME),
        config.getString(AuditDBConstants.DB_PASSWORD));
    Assert.assertTrue(connection != null);
    String dropTable = "DROP TABLE IF EXISTS " + config.getString().toUpperCase() + ";";
    String createTable =
        "CREATE TABLE "+ config.getString().toUpperCase()+"(\n TIMEINTERVAL " +
            "bigint," +
            "\n  HOSTNAME varchar(25)," +
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
    return connection;
  }

  public static void shutDownDB(Connection connection) {
    try {
      connection.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
