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
  final private String checkPointDir, checkpointKey;
  final private SimpleDateFormat dayChkFormat = new SimpleDateFormat
      ("yyyy-MM-dd");
  final private SimpleDateFormat tableDateFormat = new SimpleDateFormat
      ("yyyyMMdd");
  private boolean isFirstRun = true;
  private volatile boolean isStop = false;
  private static final Log LOG = LogFactory.getLog(AuditRollUpService.class);

  //this constructor is used for UTs
  public AuditRollUpService(ClientConfig config, boolean isFirstRun) {
    this(config);
    this.isFirstRun = isFirstRun;
  }

  public AuditRollUpService(ClientConfig config) {
    this.config = config;
    rollUpHourOfDay = config.getInteger(AuditDBConstants.ROLLUP_HOUR_KEY, 0);
    intervalLength = config.getLong(AuditDBConstants.INTERVAL_LENGTH_KEY,
        3600000l);
    checkPointDir = config.getString(AuditDBConstants.CHECKPOINT_DIR_KEY);
    checkpointKey = config.getString(AuditDBConstants.CHECKPOINT_KEY,
        AuditDBConstants.DEFAULT_CHECKPOINT_KEY);
    tilldays = config.getInteger(AuditDBConstants.TILLDAYS_KEY);
    LOG.info("Initialized AuditRollupService with configs rollup hour " +
        "as:"+rollUpHourOfDay+", interval length as:"+intervalLength+", " +
        "checkpoint directory as "+checkPointDir+" and till days as:"+tilldays);
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
   * Queries the daily table and finds the first entry ordered by timeinterval
   * Return the corresponding timeinterval
   */
  Date getFirstTimeIntervalDailyTable(Connection connection) {
    LOG.debug("Get fromTime from Table");
    String statement = "select " + AuditDBConstants.TIMESTAMP + " from " +
        config.getString(AuditDBConstants.MASTER_TABLE_NAME) + " order by " +
        "timeinterval limit 1;";
    LOG.debug("Statement to get first timeinterval in table : "+statement);
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
      LOG.error("SQLException while getting first time interval from db:"+e
          .getMessage());
    } finally {
      try {
        rs.close();
        preparedStatement.close();
      } catch (SQLException e) {
        LOG.error("SQLException while closing statement:"+e.getMessage());
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

  Date getFromTime(Connection connection) {
    FSCheckpointProvider provider = new FSCheckpointProvider(checkPointDir);
    byte[] value = provider.read(checkpointKey);
    if (value != null) {
      Long timestamp = Long.parseLong(new String(value));
      LOG.info("Get fromTime from checkpoint:"+timestamp);
      if (checkLongValOfDateValid(timestamp))
        return new Date(timestamp);
      else
        return getFirstTimeIntervalDailyTable(connection);
    } else {
      return getFirstTimeIntervalDailyTable(connection);
    }
  }

  /*
   * Return long corresponding to first millisecond of day to which date
   * belongs
   */
  Long getFirstMilliOfDay(Date date) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    return cal.getTime().getTime();
  }

  void mark(Long toTime) {
    FSCheckpointProvider provider = new FSCheckpointProvider(checkPointDir);
    provider.checkpoint(checkpointKey, toTime.toString().getBytes());
    LOG.info("Marked checkpoint to the date at which to start next run:"
        + toTime);
  }

  @Override
  public void execute() {
    if (isFirstRun) {
      LOG.info("First run: Sleeping till rollup hour");
      sleepTillNextRun();
    }

    while (!isStop && !thread.isInterrupted()) {
      LOG.info("Starting new run");
      Connection connection = getConnection();
      while (connection == null && !isStop) {
        LOG.info("Connection not initialized. Retrying.");
        connection = getConnection();
      }
      try {
        Date markTime = rollupTables(connection);
        createDailyTable(connection);
        mark(getFirstMilliOfDay(markTime));
      } finally {
        try {
          connection.close();
        } catch (SQLException e) {
          LOG.error("SQLException while closing connection:"+ e.getMessage());
        }
      }
      sleepTillNextRun();
    }
  }

  private void createDailyTable(Connection connection) {
    CallableStatement createDailyTableStmt = null;
    try {
      if (!isStop) {
        Date fromDate = new Date();
        Date todate = addDaysToCurrentDate(config.getInteger(AuditDBConstants
            .NUM_DAYS_AHEAD_TABLE_CREATION));
        String statement = getCreateTableQuery();
        createDailyTableStmt = connection.prepareCall(statement);
        String masterTable = config.getString(AuditDBConstants.MASTER_TABLE_NAME);
        while ((fromDate.before(todate) || fromDate.equals(todate)) &&
            !isStop) {
          LOG.info("Creating day table of date:"+fromDate);
          String currentDateString = dayChkFormat.format(fromDate);
          String dayTable = createTableName(fromDate, false);
          int index = 1;
          createDailyTableStmt.setString(index++, masterTable);
          createDailyTableStmt.setString(index++, dayTable);
          createDailyTableStmt.setString(index++, currentDateString);
          createDailyTableStmt.addBatch();
          fromDate = addDaysToGivenDate(fromDate, 1);
          LOG.debug("Table created for day:"+currentDateString+" with table " +
              "name as:"+dayTable+" and parent is :"+masterTable);
        }
        createDailyTableStmt.executeBatch();
        connection.commit();
      }
    } catch (SQLException e) {
      while (e != null) {
        LOG.error("SQLException while creating daily table:"+ e.getMessage());
        e = e.getNextException();
      }
    } finally {
      try {
        createDailyTableStmt.close();
      } catch (SQLException e) {
        LOG.error("SQLException while closing call statement:"+ e.getMessage
            ());
      }
    }
  }

  Date addDaysToGivenDate(Date date, int increment) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    calendar.set(Calendar.HOUR_OF_DAY, 0);
    calendar.set(Calendar.MINUTE, 0);
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MILLISECOND, 0);
    calendar.add(Calendar.DATE, increment);
    return calendar.getTime();
  }

  String getCreateTableQuery() {
    String query = "{call createDailyTable(?,?,?)}";
    return query;
  }

  Date addDaysToCurrentDate(Integer dayIncrement) {
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
      Thread.sleep(waitTime);
    } catch (InterruptedException e) {
      LOG.warn("RollUp Service interrupted", e);
    }
  }

  private Date rollupTables(Connection connection) {
    CallableStatement rollupStmt = null;
    Date fromTime = getFromTime(connection);
    try {
      if (!isStop) {
        String statement = getRollUpQuery();
        rollupStmt = connection.prepareCall(statement);
        Date toDate = addDaysToCurrentDate(-tilldays);
        LOG.info("Starting roll up of tables from:"+fromTime+" till:"+toDate);
        while (fromTime.before(toDate) && !isStop) {
          Date nextDay = addDaysToGivenDate(fromTime, 1);
          String srcTable = createTableName(fromTime, false);
          String destTable = createTableName(fromTime, true);
          Long firstMillisOfDay = getFirstMilliOfDay(fromTime);
          Long firstMillisOfNextDay = getFirstMilliOfDay(nextDay);
          int index = 1;
          rollupStmt.setString(index++, srcTable);
          rollupStmt.setString(index++, destTable);
          rollupStmt.setString(index++,
              config.getString(AuditDBConstants.MASTER_TABLE_NAME));
          rollupStmt.setLong(index++, firstMillisOfDay);
          rollupStmt.setLong(index++, firstMillisOfNextDay);
          rollupStmt.setLong(index++, intervalLength);
          LOG.debug("Rollup query is " + rollupStmt.toString());
          rollupStmt.addBatch();
          fromTime = nextDay;
        }
        rollupStmt.executeBatch();
        connection.commit();
      }
    } catch (SQLException e) {
      while (e != null) {
        LOG.error("SQLException while rolling up:"+ e.getMessage());
        e = e.getNextException();
      }
    } finally {
      try {
        rollupStmt.close();
      } catch (SQLException e) {
        LOG.error("SQLException while closing call statement:"+ e.getMessage
            ());
      }
    }
    return fromTime;
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

  String createTableName(Date currentDate, boolean isRollupTable) {
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
