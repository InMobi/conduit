package com.inmobi.conduit.visualization.server.util;

public class UtilConstants {
  public static final String CONF_FILE = "audit-feeder.properties";
  public static final String JDBC_DRIVER_CLASS_NAME = "jdbc.driver.class.name";
  public static final String DB_URL = "db.url";
  public static final String DB_USERNAME = "db.username";
  public static final String DB_PASSWORD = "db.password";
  public static final String TABLE_NAME = "audit.table.master";
  public static final String columns =
      "\n" +
          "(\n" +
          "  TIMEINTERVAL bigint,\n" +
          "  HOSTNAME varchar(50),\n" +
          "  TIER varchar(15),\n" +
          "  TOPIC varchar(25),\n" +
          "  CLUSTER varchar(50),\n" +
          "  SENT bigint,\n" +
          "  C0 bigint,\n" +
          "  C1 bigint,\n" +
          "  C2 bigint,\n" +
          "  C3 bigint,\n" +
          "  C4 bigint,\n" +
          "  C5 bigint,\n" +
          "  C6 bigint,\n" +
          "  C7 bigint,\n" +
          "  C8 bigint,\n" +
          "  C9 bigint,\n" +
          "  C10 bigint,\n" +
          "  C15 bigint,\n" +
          "  C30 bigint,\n" +
          "  C60 bigint,\n" +
          "  C120 bigint,\n" +
          "  C240 bigint,\n" +
          "  C600 bigint,\n" +
          "  PRIMARY KEY (TIMEINTERVAL,HOSTNAME,TIER,TOPIC,CLUSTER),\n" +
          ");";
}
