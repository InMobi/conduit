package com.inmobi.conduit.audit;

import com.inmobi.conduit.FSCheckpointProvider;
import com.inmobi.conduit.audit.util.AuditDBConstants;
import com.inmobi.conduit.audit.util.AuditDBHelper;
import com.inmobi.messaging.ClientConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;

public abstract class AuditDBService implements Runnable {
  // Rollup check point directory and key are added to parent class since
  // Feeder service also needs to read last rolled up table time from the
  // checkpoint to determine whether or not to add message to tables (in case
  // of messages read after rollup of that day's table i.e. late messages)
  protected String rollupChkPtDir, rollupChkPtKey;
  protected final ClientConfig config;
  protected FSCheckpointProvider rollupProvider;
  protected boolean isDailyRollup = false;
  protected int hourlyTilldays, dailyTillDays;

  protected Thread thread;
  protected volatile boolean isStop = false;
  private static final Log LOG = LogFactory.getLog(AuditDBService.class);

  public AuditDBService (ClientConfig config) {
    this(config, false);
  }

  public AuditDBService(ClientConfig config, boolean isDailyRollup) {
    this.isDailyRollup = isDailyRollup;
    this.config = config;
    hourlyTilldays = config.getInteger(AuditDBConstants.TILLDAYS_KEY,
        AuditDBConstants.DEFAULT_HOURLY_ROLLUP_TILLDAYS);
    dailyTillDays = config.getInteger(AuditDBConstants
        .DAILY_ROLLUP_TILLDAYS_KEY, hourlyTilldays + AuditDBConstants
        .DEFAULT_GAP_BTW_ROLLUP_TILLDAYS);
    if (dailyTillDays <= hourlyTilldays) {
      LOG.error("Passed configs for daily.rollup.tilldays[" + dailyTillDays +
      "] is less than rollup.tilldays[" + hourlyTilldays + "]");
      dailyTillDays = hourlyTilldays + AuditDBConstants.DEFAULT_GAP_BTW_ROLLUP_TILLDAYS;
      LOG.info("Reset daily.rollup.tilldays to " + dailyTillDays);
    }
    rollupChkPtDir = config.getString(AuditDBConstants.CHECKPOINT_DIR_KEY);
    if (isDailyRollup) {
      rollupChkPtKey = config.getString(AuditDBConstants.DAILY_ROLLUP_CHECKPOINT_KEY,
          AuditDBConstants.DEFAULT_DAILY_CHECKPOINT_KEY);
      if (rollupChkPtKey.length() == 0) {
        rollupChkPtKey = AuditDBConstants.DEFAULT_DAILY_CHECKPOINT_KEY;
      }
    } else {
      rollupChkPtKey = config.getString(AuditDBConstants.CHECKPOINT_KEY,
          AuditDBConstants.DEFAULT_CHECKPOINT_KEY);
      if (rollupChkPtKey.length() == 0) {
        rollupChkPtKey = AuditDBConstants.DEFAULT_CHECKPOINT_KEY;
      }
    }
    if (rollupChkPtDir != null && rollupChkPtDir.length() > 0) {
      rollupProvider = new FSCheckpointProvider(rollupChkPtDir);
    }
  }

  @Override
  public void run() {
    execute();
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
        if ((isDailyRollup && firstDate.after(AuditDBHelper
            .addDaysToCurrentDate(-dailyTillDays))) || (!isDailyRollup &&
            firstDate.after(AuditDBHelper.addDaysToCurrentDate
                (-hourlyTilldays)))) {
          LOG.error("First Date of the table is after the till days limit " +
              "for the service, returning null start date");
          return null;
        }
        Date currentDate = lastDate;
        while (!currentDate.before(firstDate)) {
          if (checkTableExists(connection, createTableName(currentDate, true,
              isDailyRollup)))
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

  public String createTableName(Date currentDate, boolean isRollupTable) {
    return createTableName(currentDate, isRollupTable, false);
  }

  public String createTableName(Date currentDate, boolean isRollupTable,
                                boolean isDailyRolledUpTable) {
    String tableName = "";
    if (isRollupTable) {
      if (isDailyRolledUpTable) {
        tableName += "daily_";
      } else {
        tableName += "hourly_";
      }
    }
    tableName += config.getString(AuditDBConstants.MASTER_TABLE_NAME) +
        AuditDBHelper.TABLE_DATE_FORMATTER.format(currentDate);
    return tableName;
  }
}
