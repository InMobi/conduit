package com.inmobi.databus.visualization.server.util;

public class UtilConstants {
  public static final String CONF_FILE = "audit-feeder.properties";
  public static final String JDBC_DRIVER_CLASS_NAME = "jdbc.driver.class.name";
  public static final String DB_URL = "db.url";
  public static final String DB_USERNAME = "db.username";
  public static final String DB_PASSWORD = "db.password";
  public static final String TABLE_NAME = "audit.table.master";
  public static final String columns =
      "(TIMEINTERVAL bigint, HOSTNAME varchar(50), TIER varchar(15), " +
          "TOPIC varchar(25), CLUSTER varchar(50), SENT bigint, C0 bigint, " +
          "C2 bigint, C1 bigint, C3 bigint, C4 bigint, C5 bigint, C6 bigint, " +
          "C7 bigint, C8 bigint, C9 bigint, C10 bigint, C15 bigint, " +
          "C30 bigint, C60 bigint, C120 bigint, C240 bigint, C600 bigint, " +
          "PRIMARY KEY (TIMEINTERVAL,HOSTNAME,TIER,TOPIC,CLUSTER))";
}
