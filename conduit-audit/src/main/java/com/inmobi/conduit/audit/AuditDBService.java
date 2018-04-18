package com.inmobi.conduit.audit;

import com.inmobi.conduit.FSCheckpointProvider;
import com.inmobi.conduit.audit.util.AuditDBConstants;
import com.inmobi.conduit.audit.util.AuditDBHelper;
import com.inmobi.messaging.ClientConfig;
import org.apache.commons.lang.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

public abstract class AuditDBService implements Runnable {
  // Rollup check point directory and key are added to parent class since
  // Feeder service also needs to read last rolled up table time from the
  // checkpoint to determine whether or not to add message to tables (in case
  // of messages read after rollup of that day's table i.e. late messages)
  protected String rollupChkPtDir, rollupChkPtKey;
  protected final ClientConfig config;
  protected FSCheckpointProvider rollupProvider;
  protected int tillDays;

  protected Thread thread;
  protected volatile boolean isStop = false;
  private static final Log LOG = LogFactory.getLog(AuditDBService.class);

  public AuditDBService(ClientConfig config) {
    this.config = config;
    tillDays = config.getInteger(AuditDBConstants.TILLDAYS_KEY,
        AuditDBConstants.DEFAULT_HOURLY_ROLLUP_TILLDAYS);
    rollupChkPtDir = config.getString(AuditDBConstants.CHECKPOINT_DIR_KEY);
    rollupChkPtKey = config.getString(AuditDBConstants.CHECKPOINT_KEY,
        AuditDBConstants.DEFAULT_CHECKPOINT_KEY);
    if (rollupChkPtDir != null && rollupChkPtDir.length() > 0) {
      rollupProvider = new FSCheckpointProvider(rollupChkPtDir);
    }
  }

  @Override
  public void run() {
    boolean isKerberoseEnabled = config.getBoolean(AuditDBConstants.KERBEROSE_ENABLED_KEY,
            AuditDBConstants.DEFAULT_KERBEROSE_ENABLED_VALUE);

    LOG.info("Kerberose Authentication : " + isKerberoseEnabled);

    if (isKerberoseEnabled) {
      LOG.info("Starting timertask for KDC ticket refresh.");
      refereshKDCTicket();
    }

    execute();
  }

  private void refereshKDCTicket() {
    try {
      Timer t = new Timer();

      int interval = config.getInteger(AuditDBConstants.KDC_REFERESH_KEY,
              AuditDBConstants.DEFAULT_KDC_REFERESH_VALUE_MINUTES);

      final String principal = config.getString(AuditDBConstants.KDC_PRINCIPAL);
      final String keytabFilePath = config.getString(AuditDBConstants.KDC_KEYTAB);

      LOG.info("KDC ticket referesh interval : " + interval);

      t.scheduleAtFixedRate(new TimerTask() {

        @Override
        public void run() {
          try {
            refreshLensTGT(principal, keytabFilePath);
          } catch (Exception e) {
            LOG.error("Unable to referesh KDC ticket... " + e.toString());
        }
      }

    },0,interval);

    } catch (Exception ex) {
      LOG.error("Unable to start KDC refresh thread, " + ex.toString());
      throw new RuntimeException(ex);
    }
  }

  private void refreshLensTGT(String principal, String keytabFilePath) throws IOException, IllegalArgumentException {

    Configuration hadoopConf = new Configuration();
    hadoopConf.set("hadoop.security.authentication", "kerberos");

    UserGroupInformation.setConfiguration(hadoopConf);

    UserGroupInformation.loginUserFromKeytab(principal, keytabFilePath);

    LOG.info("Got Kerberos ticket, keytab: " + keytabFilePath + ", Lens principal: " + principal);

  }


  public void start() {
    thread = new Thread(this, getServiceName());
    LOG.info("Starting thread " + thread.getName());
    thread.start();
  }

  public void join() {
    try {
      thread.join();
    } catch (InterruptedException e) {
      LOG.error("Exception while waiting for thread " + thread.getName()
          + " to join", e);
    }
  }

  public abstract void stop();

  public abstract void execute();

  public abstract String getServiceName();

  public Date getRollupTime() {
    Date rollupTime = getRollupTimeFromChkPt();
    if (rollupTime == null) {
      return getRollupTimeFromDB();
    } else {
      return rollupTime;
    }
  }

  protected Date getRollupTimeFromChkPt() {
    try {
      byte[] value = rollupProvider.read(rollupChkPtKey);
      if (value != null) {
        Long timestamp;
        try {
          timestamp = Long.parseLong(new String(value));
        } catch (NumberFormatException e) {
          LOG.error("Unparseable rollup timestamp value from rollup " +
              "checkpoint:" + value, e);
          return null;
        }
        LOG.info("Get rollup fromTime from checkpoint:" + timestamp);
        if (checkLongValOfDateValid(timestamp)) {
          return new Date(timestamp);
        }
      }
    } catch (Exception e) {
      LOG.error("Exception while reading from checkpoint", e);
      return null;
    }
    return null;
  }

  private boolean checkLongValOfDateValid(Long timestamp) {
    Calendar calendar = Calendar.getInstance();
    try {
      calendar.setTime(new Date(timestamp));
    } catch (Exception e) {
      LOG.debug("Invalid timestamp from checkpoint:" + timestamp);
      return false;
    }
    LOG.debug("Valid timestamp from checkpoint");
    return true;
  }

  private Date getRollupTimeFromDB() {
    LOG.debug("Get fromTime from Table");
    Connection connection = null;
    try {
      while (connection == null && !isStop) {
        LOG.warn("Connection is null when getting next rollup time from DB");
        connection = AuditDBHelper.getConnection(config);
      }
      Date firstDate = new Date(AuditDBHelper.getFirstMilliOfDay
          (getTimeEnrtyDailyTable(connection, true)));
      Date lastDate = new Date(AuditDBHelper.getFirstMilliOfDay
          (getTimeEnrtyDailyTable(connection, false)));
      if (firstDate != null && lastDate != null) {
        LOG.debug("Table dates corresponding to first entry:" +
            AuditDBHelper.DAY_CHK_FORMATTER.format(firstDate) + " and last " +
            "entry:" + AuditDBHelper.DAY_CHK_FORMATTER.format(lastDate));
        if (firstDate.after(AuditDBHelper.addDaysToCurrentDate(-tillDays))) {
          LOG.error("First Date of the table is after the till days limit " +
              "for the service" + AuditDBHelper.addDaysToCurrentDate
              (-tillDays) + ", returning null start date");
          return null;
        }
        Date currentDate = lastDate;
        while (!currentDate.before(firstDate)) {
          if (checkTableExists(connection, createRolledTableNameForService(currentDate)))
            return AuditDBHelper.addDaysToGivenDate(currentDate, 1);
          currentDate = AuditDBHelper.addDaysToGivenDate(currentDate, -1);
        }
        return firstDate;
      } else {
        return null;
      }
    } finally {
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException e) {
          AuditDBHelper.logNextException("SQLException while closing db " +
              "connection", e);
        }
      }
    }
  }

  /*
   * Queries the daily table and finds the first/last entry ordered by
   * timeinterval depending on isAsc
   */
  public Date getTimeEnrtyDailyTable(Connection connection, boolean isAsc) {

    String statement = "select " + AuditDBConstants.TIMESTAMP + " from " +
        config.getString(AuditDBConstants.MASTER_TABLE_NAME) + " order by " +
        "timeinterval";
    if (isAsc) {
      statement += " asc limit 1;";
    } else {
      statement += " desc limit 1;";
    }
    LOG.debug("Statement to get first/last timeinterval in table : " +
        statement);

    PreparedStatement preparedStatement = null;
    ResultSet rs = null;
    Long result = 0l;
    try {
      preparedStatement = connection.prepareStatement(statement);
      preparedStatement.execute();
      rs = preparedStatement.getResultSet();
      if (rs.next())
        result = rs.getLong(AuditDBConstants.TIMESTAMP);
    } catch (SQLException e) {
      AuditDBHelper.logNextException("SQLException while getting first/last time interval" +
          " from db:", e);
      return null;
    } finally {
      try {
        if (rs != null)
          rs.close();
        if (preparedStatement != null)
          preparedStatement.close();
      } catch (SQLException e) {
        AuditDBHelper.logNextException("SQLException while closing statement:", e);
      }
    }
    return new Date(result);
  }

  /**
   * public helper method used by bothe rollup service and admin script tp
   * check if table exists in db
   * @param tableName table name for whose existence to check for
   * @return true if table 'tablename' exists else false
   */
  protected boolean checkTableExists(Connection connection, String tableName) {
    String statement = "select table_name from information_schema.tables " +
        "where table_name = '" + tableName +"';";
    PreparedStatement preparedStatement = null;
    ResultSet rs = null;
    try {
      preparedStatement = connection.prepareStatement(statement);
      rs = preparedStatement.executeQuery();
      if (rs.next()) {
        LOG.info("Table: " + tableName + " exists");
        return true;
      }
    } catch (SQLException e) {
      AuditDBHelper.logNextException("Exception while checking for table", e);
    } finally {
      try {
        if (rs != null)
          rs.close();
        if (preparedStatement != null)
          preparedStatement.close();
      } catch (SQLException e) {
        AuditDBHelper.logNextException("SQLException while closing resultset/statement", e);
      }
    }
    LOG.info("Table: " + tableName + " does not exists");
    return false;
  }

  public static String createMinuteTableName(ClientConfig config, Date date) {
    return config.getString(AuditDBConstants.MASTER_TABLE_NAME) +
        AuditDBHelper.TABLE_DATE_FORMATTER.format(date);
  }

  public static String createHourTableName(ClientConfig config, Date date) {
    return "hourly_" + config.getString(AuditDBConstants
        .MASTER_TABLE_NAME) + AuditDBHelper.TABLE_DATE_FORMATTER.format(date);
  }

  public static String createDailyTableName(ClientConfig config, Date date) {
    return "daily_" + config.getString(AuditDBConstants
        .MASTER_TABLE_NAME) + AuditDBHelper.TABLE_DATE_FORMATTER.format(date);
  }

  public String createRolledTableNameForService(Date date) {
    return createHourTableName(config, date);
  }
}
