package com.inmobi.databus.audit.services;

import com.inmobi.databus.FSCheckpointProvider;
import com.inmobi.databus.audit.AuditDBService;
import com.inmobi.databus.audit.util.AuditDBConstants;
import com.inmobi.databus.audit.util.AuditDBHelper;
import com.inmobi.messaging.ClientConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class AuditRollUpService extends AuditDBService {

  final private ClientConfig config;
  final private int rollUpHourOfDay, tilldays;
  final private long intervalLength;
  final private String checkPointDir, checkpointKey, masterTable;
  final private SimpleDateFormat dayChkFormat = new SimpleDateFormat
      ("yyyy-MM-dd");
  final private SimpleDateFormat tableDateFormat = new SimpleDateFormat
      ("yyyyMMdd");
  private final FSCheckpointProvider provider;
  private static final Log LOG = LogFactory.getLog(AuditRollUpService.class);

  public AuditRollUpService(ClientConfig config) {
    this.config = config;
    rollUpHourOfDay = config.getInteger(AuditDBConstants.ROLLUP_HOUR_KEY, 0);
    intervalLength = config.getLong(AuditDBConstants.INTERVAL_LENGTH_KEY,
        3600000l);
    checkPointDir = config.getString(AuditDBConstants.CHECKPOINT_DIR_KEY);
    checkpointKey = config.getString(AuditDBConstants.CHECKPOINT_KEY,
        AuditDBConstants.DEFAULT_CHECKPOINT_KEY);
    masterTable = config.getString(AuditDBConstants.MASTER_TABLE_NAME);
    tilldays = config.getInteger(AuditDBConstants.TILLDAYS_KEY);
    provider = new FSCheckpointProvider(checkPointDir);
    LOG.info("Initialized AuditRollupService with configs rollup hour " +
        "as:" + rollUpHourOfDay + ", interval length as:" + intervalLength +
        ", checkpoint directory as " + checkPointDir + " and till days as:" +
        tilldays);
  }

  @Override
  public void stop() {
    isStop = true;
    //RollupService sleeps for a day between runs so have to interrupt the
    // thread on calling stop()
    thread.interrupt();
  }

  private long getTimeToSleep() {
    Calendar cal = Calendar.getInstance();
    long currentTime = cal.getTimeInMillis();
    // setting calendar to rollup hour
    if (cal.get(Calendar.HOUR_OF_DAY) >= rollUpHourOfDay) {
      // rollup will happen the next day
      cal.add(Calendar.DATE, 1);
    }
    cal.set(Calendar.HOUR_OF_DAY, rollUpHourOfDay);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    long rollUpTime = cal.getTimeInMillis();
    return rollUpTime - currentTime;
  }

  private String getRollUpQuery() {
    String query = "{call rollup(?,?,?,?,?,?)}";
    return query;
  }

  /*
   * Queries the daily table and finds the first/last entry ordered by
   * timeinterval depending on isAsc
   */
  protected Date getTimeEnrtyDailyTable(Connection connection, boolean isAsc) {

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

  private boolean checkLongValOfDateValid(Long timestamp) {
    Calendar calendar = Calendar.getInstance();
    try {
      calendar.setTime(new Date(timestamp));
    } catch (Exception e) {
      return false;
    }
    return true;
  }

  protected Date getFromTime(Connection connection) {
    byte[] value = provider.read(checkpointKey);
    if (value != null) {
      Long timestamp;
      try {
        timestamp = Long.parseLong(new String(value));
      } catch (NumberFormatException e) {
        LOG.error("Unparseable timestamp value from checkpoint:" + value, e);
        return getFromTimeFromDB(connection);
      }
      LOG.info("Get fromTime from checkpoint:"+timestamp);
      if (checkLongValOfDateValid(timestamp))
        return new Date(timestamp);
    }
    return getFromTimeFromDB(connection);
  }

  private Date getFromTimeFromDB(Connection connection) {
    LOG.debug("Get fromTime from Table");
    Date firstDate = getTimeEnrtyDailyTable(connection, true);
    Date lastDate = getTimeEnrtyDailyTable(connection, false);
    Date currentDate = lastDate;
    while (!currentDate.before(firstDate)) {
      if (checkTableExists(connection, createTableName(currentDate, true)))
        return addDaysToGivenDate(currentDate, 1);
      currentDate = addDaysToGivenDate(currentDate, -1);
    }
    return firstDate;
  }

  public boolean checkTableExists(Connection connection, String tableName) {
    String statement = "select table_name from information_schema.tables " +
        "where table_name = '" + tableName +"';";
    PreparedStatement preparedStatement = null;
    ResultSet rs = null;
    try {
      preparedStatement = connection.prepareStatement(statement);
      rs = preparedStatement.executeQuery();
      if (rs.next())
        return true;
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
    return false;
  }

  /*
   * Return long corresponding to first millisecond of day to which date
   * belongs
   */
  private Long getFirstMilliOfDay(Date date) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    return cal.getTime().getTime();
  }

  public void mark(Long toTime) {
    try {
      provider.checkpoint(checkpointKey, toTime.toString().getBytes());
      LOG.info("Marked checkpoint to the date at which to start next run:"
          + toTime);
    } catch (Exception e) {
      LOG.error("Marking checkpoint failed", e);
    }
  }

  @Override
  public void execute() {

    while (!isStop && !thread.isInterrupted()) {
      LOG.info("Starting new run");
      Connection connection = getConnection();
      while (connection == null && !isStop) {
        LOG.info("Connection not initialized. Retry after 5 minutes");
        try {
          Thread.sleep(300000l);
        } catch (InterruptedException e) {
          LOG.error("Interrupted before connecting to db", e);
        }
        LOG.info("Retrying to establish connection.");
        if (!isStop) {
          connection = getConnection();
        }
      }
      LOG.info("Connection initialized");

      try {
        if (!isStop) {
          createDailyTable(connection);
          Date markTime = rollupTables(connection);
          if (markTime != null) {
            mark(getFirstMilliOfDay(markTime));
          }
        }
      } catch (SQLException e) {
        AuditDBHelper.logNextException("SQLException while rollup up tables", e);
      } finally {
        try {
          if (connection != null)
            connection.close();
        } catch (SQLException e) {
          AuditDBHelper.logNextException("SQLException while closing connection:", e);
        }
      }
      sleepTillNextRun();
    }
  }

  private void createDailyTable(Connection connection) {
    if (!isStop) {
      Date fromDate = new Date();
      Date todate = addDaysToCurrentDate(config.getInteger(AuditDBConstants
          .NUM_DAYS_AHEAD_TABLE_CREATION));
      createDailyTable(fromDate, todate, connection);
    }
  }

  public boolean createDailyTable(Date fromDate, Date todate,
                                  Connection connection) {
    CallableStatement createDailyTableStmt = null;
    try {
      if (!isStop) {
        String statement = getCreateTableQuery();
        createDailyTableStmt = connection.prepareCall(statement);
        int addedToBatch = 0;
        while (!fromDate.after(todate) && !isStop) {
          LOG.info("Creating day table of date:"+fromDate);
          String currentDateString = dayChkFormat.format(fromDate);
          String nextDayString = dayChkFormat.format(addDaysToGivenDate
              (fromDate, 1));
          String dayTable = createTableName(fromDate, false);
          int index = 1;
          createDailyTableStmt.setString(index++, masterTable);
          createDailyTableStmt.setString(index++, dayTable);
          createDailyTableStmt.setString(index++, currentDateString);
          createDailyTableStmt.setString(index++, nextDayString);
          createDailyTableStmt.addBatch();
          LOG.debug("Table added to batch for day:"+currentDateString+" with " +
              "table name as:"+dayTable+" and parent is :"+masterTable);
          addedToBatch++;
          fromDate = addDaysToGivenDate(fromDate, 1);
        }
        int[] retVal = createDailyTableStmt.executeBatch();
        if (retVal.length != addedToBatch) {
          LOG.error("Mismatch in number of tables added to batch[" +
              addedToBatch + "] and rolledup tables[" + retVal.length + "]");
        }
        connection.commit();
      }
    } catch (SQLException e) {
      AuditDBHelper.logNextException("SQLException while creating daily table", e);
      return false;
    } finally {
      try {
        if (createDailyTableStmt != null)
          createDailyTableStmt.close();
      } catch (SQLException e) {
        AuditDBHelper.logNextException("SQLException while closing call statement:", e);
      }
    }
    return true;
  }

  public Date addDaysToGivenDate(Date date, int increment) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    calendar.set(Calendar.HOUR_OF_DAY, 0);
    calendar.set(Calendar.MINUTE, 0);
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MILLISECOND, 0);
    calendar.add(Calendar.DATE, increment);
    return calendar.getTime();
  }

  private String getCreateTableQuery() {
    String query = "{call createDailyTable(?,?,?,?)}";
    return query;
  }

  public Date addDaysToCurrentDate(Integer dayIncrement) {
    Calendar calendar = Calendar.getInstance();
    calendar.set(Calendar.HOUR_OF_DAY, 0);
    calendar.set(Calendar.MINUTE, 0);
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MILLISECOND, 0);
    calendar.add(Calendar.DATE, dayIncrement);
    return calendar.getTime();
  }

  private void sleepTillNextRun() {
    // sleep till next roll up hour
    long waitTime = getTimeToSleep();
    LOG.info("Sleeping for "+waitTime+"ms");
    try {
      if (!isStop) {
        Thread.sleep(waitTime);
      }
    } catch (InterruptedException e) {
      LOG.warn("RollUp Service interrupted", e);
    }
  }

  private Date rollupTables(Connection connection) throws SQLException {
    if (!isStop) {
      Date fromTime = getFromTime(connection);
      Date toDate = addDaysToCurrentDate(-tilldays);
      if (!fromTime.after(toDate))
        return rollupTables(fromTime, toDate, connection);
    }
    return null;
  }

  public Date rollupTables(Date fromTime, Date toDate,
                           Connection connection) throws SQLException {
    CallableStatement rollupStmt = null;
    Date currentDate = fromTime;
    try {
      if (!isStop) {
        String statement = getRollUpQuery();
        rollupStmt = connection.prepareCall(statement);
        int addedToBatch = 0;
        LOG.info("Starting roll up of tables from:"+currentDate+" till:"+toDate);
        while (currentDate.before(toDate) && !isStop) {
          Date nextDay = addDaysToGivenDate(currentDate, 1);
          String srcTable = createTableName(currentDate, false);
          String destTable = createTableName(currentDate, true);
          Long firstMillisOfDay = getFirstMilliOfDay(currentDate);
          Long firstMillisOfNextDay = getFirstMilliOfDay(nextDay);
          int index = 1;
          rollupStmt.setString(index++, srcTable);
          rollupStmt.setString(index++, destTable);
          rollupStmt.setString(index++, masterTable);
          rollupStmt.setLong(index++, firstMillisOfDay);
          rollupStmt.setLong(index++, firstMillisOfNextDay);
          rollupStmt.setLong(index++, intervalLength);
          LOG.debug("Rollup query is " + rollupStmt.toString());
          rollupStmt.addBatch();
          addedToBatch++;
          currentDate = nextDay;
        }
        int[] retVal = rollupStmt.executeBatch();
        if (retVal.length != addedToBatch) {
          LOG.error("Mismatch in number of tables added to batch[" +
              addedToBatch + "] and rolledup tables[" + retVal.length + "]");
        }
        connection.commit();
      }
    } finally {
      if (rollupStmt != null) {
        try {
          rollupStmt.close();
        } catch (SQLException e) {
          AuditDBHelper.logNextException("SQLException while closing call statement:", e);
        }
      }
    }
    return currentDate;
  }

  private Connection getConnection() {
    LOG.info("Connecting to DB ...");
    Connection connection = AuditDBHelper.getConnection(
        config.getString(AuditDBConstants.JDBC_DRIVER_CLASS_NAME),
        config.getString(AuditDBConstants.DB_URL),
        config.getString(AuditDBConstants.DB_USERNAME),
        config.getString(AuditDBConstants.DB_PASSWORD));
    LOG.info("Connected to DB");
    return connection;
  }

  public String createTableName(Date currentDate, boolean isRollupTable) {
    String tableName = "";
    if (isRollupTable) {
      tableName += "hourly_";
    }
    tableName += config.getString(AuditDBConstants.MASTER_TABLE_NAME) +
        tableDateFormat.format(currentDate);
    return tableName;
  }

  @Override
  public String getServiceName() {
    return "RollUpService";
  }
}
