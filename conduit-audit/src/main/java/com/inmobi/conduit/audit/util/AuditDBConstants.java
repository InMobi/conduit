package com.inmobi.conduit.audit.util;

public interface AuditDBConstants {

  public static final String FEEDER_CONF_FILE = "audit-feeder.properties";

  public static final String JDBC_DRIVER_CLASS_NAME = "jdbc.driver.class.name";
  public static final String DB_URL = "db.url";
  public static final String DB_USERNAME = "db.username";
  public static final String DB_PASSWORD = "db.password";
  public static final String MASTER_TABLE_NAME = "audit.table.master";

  public static final String TIMESTAMP = "TIMEINTERVAL";
  public static final String SENT = "SENT";

  public static final String GANGLIA_HOST = "feeder.ganglia.host";
  public static final String GANGLIA_PORT = "feeder.ganglia.port";
  public static final String CSV_REPORT_DIR = "feeder.csv.report.dir";
  public static final String MESSAGES_PER_BATCH = "messages.batch.num";
  public static final String CONDUIT_CONF_FILE_KEY = "feeder.conduit.conf";

  public static final String ROLLUP_HOUR_KEY = "rollup.hour";
  public static final String INTERVAL_LENGTH_KEY = "rollup.intervallength.millis";
  public static final String CHECKPOINT_DIR_KEY = "rollup.checkpoint.dir";
  public static final String CHECKPOINT_KEY = "rollup.checkpoint.key";
  public static final String TILLDAYS_KEY = "rollup.tilldays";
  public static final String NUM_DAYS_AHEAD_TABLE_CREATION =
      "num.of.days.ahead.table.creation";

  public static final String DAILY_ROLLUP_CHECKPOINT_KEY = "daily.rollup" +
      ".checkpoint.key";
  public static final String DAILY_ROLLUP_TILLDAYS_KEY = "daily.rollup" +
      ".tilldays";

  public static final String DEFAULT_CHECKPOINT_KEY = "rollupChkPt";
  public static final String DEFAULT_DAILY_CHECKPOINT_KEY = "dailyRollupChkPt";
  public static final int DEFAULT_GAP_BTW_ROLLUP_TILLDAYS = 30;
  public static final int DEFAULT_HOURLY_ROLLUP_TILLDAYS = 5;

  public static final String KDC_REFERESH_KEY = "kdc.referesh.interval.miniutes";
  public static final int DEFAULT_KDC_REFERESH_VALUE_MINUTES = 360;
  public static final String KERBEROSE_ENABLED_KEY = "kerberos.isenabled";
  public static final boolean DEFAULT_KERBEROSE_ENABLED_VALUE = false;
  public static final String KDC_PRINCIPAL = "kdc.principal";
  public static final String KDC_KEYTAB = "kdc.keytab";

}
