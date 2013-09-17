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
    // since DB handles transactions and all operations are done in 1 single
    // transaction so no need to implement stop
  }

  private long getTimeToSleep() {
    Calendar cal = Calendar.getInstance();
    long currentTime = cal.getTimeInMillis();
    // setting calendar to rollup hour
    if (cal.get(Calendar.HOUR_OF_DAY) >= rollUpHourOfDay) {
      // rollup will happen the next day
      cal.add(Calendar.DAY_OF_MONTH, 1);
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
  public Date getFirstTimeIntervalDailyTable(Connection connection)
      throws SQLException {
    String statement = "select " + AuditDBConstants.TIMESTAMP + " from " +
        config.getString(AuditDBConstants.MASTER_TABLE_NAME) + " order by " +
        "timeinterval limit 1;";
    LOG.debug("Statement to get first timeinterval in table : "+statement);
    PreparedStatement preparedStatement = connection.prepareStatement(statement);
    preparedStatement.execute();
    Long result = 0l;
    ResultSet rs = preparedStatement.getResultSet();
    if (rs.next())
      result = rs.getLong(AuditDBConstants.TIMESTAMP);
    return new Date(result);
  }

  public Date getFromTime(Connection connection) throws SQLException {
    FSCheckpointProvider provider = new FSCheckpointProvider(checkPointDir);
    byte[] value = provider.read(checkpointKey);
    if (value == null) {
      LOG.debug("Get fromTime from Table");
      return getFirstTimeIntervalDailyTable(connection);
    }
    LOG.debug("Get fromTime from checkpoint");
    return new Date(Long.parseLong(new String(value)));
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

  public void mark(Long toTime) throws SQLException {
    LOG.debug("Marking checkpoint to the date at which to start next run " +
        "at:"+toTime);
    FSCheckpointProvider provider = new FSCheckpointProvider(checkPointDir);
    provider.checkpoint(checkpointKey, toTime.toString().getBytes());
  }

  @Override
  public void execute() {

    if (isFirstRun) {
      sleepTillNextRun();
    }

    LOG.debug("Starting new run");
    Connection connection = getConnection();
    if (connection == null) {
      LOG.error("Connection not initialized returning ...");
      return;
    }
    try {
      rollupTables(connection);
      createDailyTable(connection);
    } finally {
      try {
        connection.close();
      } catch (SQLException e) {
        LOG.error("SQLException while closing connection:"+ e.getMessage());
      }
    }
    sleepTillNextRun();
  }

  private void createDailyTable(Connection connection) {
    CallableStatement createDailyTableStmt = null;
    try {
      LOG.info("Creating next day's minute table");
      Date date = addToCurrentDate(config.getInteger(AuditDBConstants
          .NUM_DAYS_AHEAD_TABLE_CREATION));
      String statement = getCreateTableQuery();
      createDailyTableStmt = connection.prepareCall(statement);
      String currentDateString = dayChkFormat.format(date);
      String masterTable = config.getString(AuditDBConstants.MASTER_TABLE_NAME);
      String dayTable = createTableName(date, false);
      int index = 1;
      createDailyTableStmt.setString(index++, masterTable);
      createDailyTableStmt.setString(index++, dayTable);
      createDailyTableStmt.setString(index++, currentDateString);
      createDailyTableStmt.execute();
      connection.commit();
      LOG.info("Table created for day:"+currentDateString+" with table name " +
          "as:"+dayTable+" and parent is :"+masterTable);
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

  public Date addToDate(Date date, int increment) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    calendar.add(Calendar.DATE, increment);
    return calendar.getTime();
  }

  private String getCreateTableQuery() {
    String query = "{call createDailyTable(?,?,?)}";
    return query;
  }

  public Date addToCurrentDate(Integer dayIncrement) {
    Calendar calendar = Calendar.getInstance();
    calendar.add(Calendar.DATE, dayIncrement);
    return calendar.getTime();
  }

  private void sleepTillNextRun() {
    // sleep till next roll up hour
    long waitTime = getTimeToSleep();
    LOG.debug("Sleeping for "+waitTime+"ms");
    try {
      Thread.sleep(waitTime);
    } catch (InterruptedException e) {
      LOG.warn("RollUp Service interrupted", e);
    }
  }

  private void rollupTables(Connection connection) {
    CallableStatement rollupStmt = null;
    try {
      Date fromTime = getFromTime(connection);
      String statement = getRollUpQuery();
      rollupStmt = connection.prepareCall(statement);
      Date toDate = addToCurrentDate(-tilldays);
      LOG.debug("Starting roll up of tables from:"+fromTime+" till:"+toDate);
      while (fromTime.before(toDate)) {
        Date nextDay = addToDate(fromTime, 1);
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
      mark(getFirstMilliOfDay(fromTime));
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
