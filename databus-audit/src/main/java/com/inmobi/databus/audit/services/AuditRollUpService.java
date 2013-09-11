package com.inmobi.databus.audit.services;


import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.inmobi.databus.FSCheckpointProvider;
import com.inmobi.databus.audit.AuditDBService;
import com.inmobi.databus.audit.util.AuditDBConstants;
import com.inmobi.databus.audit.util.AuditDBHelper;
import com.inmobi.messaging.ClientConfig;

public class AuditRollUpService extends AuditDBService {

  final private ClientConfig config;
  final private int rollUpHourOfDay, tilldays;
  final private long intervalLength;
  final private String checkPointDir, checkpointKey;
  final private SimpleDateFormat tableDateFormat = new SimpleDateFormat
      ("yyyyMMdd");
  private boolean startRun = true;/*
  private boolean stopAfterOneRun = false;*/
  private static final Log LOG = LogFactory.getLog(AuditRollUpService.class);

  //this constructor is used for UTs
  public AuditRollUpService(ClientConfig config, boolean startRun) {
    this(config);
    this.startRun = startRun;/*
    if (!startRun)
      stopAfterOneRun = true;*/
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

  public void mark(Long toTime)
      throws SQLException {
    LOG.debug("Marking checkpoint to the date at which to start next run " +
        "at:"+toTime);
    FSCheckpointProvider provider = new FSCheckpointProvider(checkPointDir);
    provider.checkpoint(checkpointKey, toTime.toString().getBytes());
  }

  @Override
  public void execute() {

    if (startRun) {
      //wait till rollup hour
      long waitTime = getTimeToSleep();
      LOG.debug("First run: sleeping for "+waitTime+"ms");
      try {
        Thread.sleep(waitTime);
      } catch (InterruptedException e) {
        LOG.warn("RollUp Service interrupted", e);
      }
      startRun = false;
    }

    LOG.debug("Starting new run");
    LOG.info("Connecting to DB ...");
    Connection connection = AuditDBHelper.getConnection(
        config.getString(AuditDBConstants.JDBC_DRIVER_CLASS_NAME),
        config.getString(AuditDBConstants.DB_URL),
        config.getString(AuditDBConstants.DB_USERNAME),
        config.getString(AuditDBConstants.DB_PASSWORD));
    if (connection == null) {
      LOG.error("Connection not initialized returning ...");
      return;
    }
    LOG.info("Connected to DB");

    Date fromTime;
    CallableStatement callableStatement = null;
    try {
      fromTime = getFromTime(connection);
      LOG.debug("Starting roll up of tables from:"+fromTime);
      String statement = getRollUpQuery();
      callableStatement = connection.prepareCall(statement);
      Calendar calendar = Calendar.getInstance();
      calendar.add(Calendar.DAY_OF_YEAR, -tilldays);
      Date toDate = calendar.getTime();
      calendar.setTime(fromTime);
      Date currentDate = calendar.getTime();
      LOG.debug("Start date:"+currentDate);
      LOG.debug("End up till:"+toDate);
      while (currentDate.before(toDate)) {
        calendar.add(Calendar.DAY_OF_YEAR, 1);
        Date nextDay = calendar.getTime();
        String srcTable = createTableName(currentDate, false);
        String destTable = createTableName(currentDate, true);
        Long firstMillisOfDay = getFirstMilliOfDay(currentDate);
        Long firstMillisOfNextDay = getFirstMilliOfDay(nextDay);
        int index = 1;
        callableStatement.setString(index++, srcTable);
        callableStatement.setString(index++, destTable);
        callableStatement.setString(index++,
            config.getString(AuditDBConstants.MASTER_TABLE_NAME));
        callableStatement.setLong(index++, firstMillisOfDay);
        callableStatement.setLong(index++, firstMillisOfNextDay);
        callableStatement.setLong(index++, intervalLength);
        LOG.debug("Rollup query is " + callableStatement.toString());
        callableStatement.addBatch();
        currentDate = nextDay;
      }
      callableStatement.executeBatch();
      connection.commit();
      mark(getFirstMilliOfDay(calendar.getTime()));
    } catch (SQLException e) {
      while (e != null) {
        LOG.error("SQLException while rolling up:"+ e.getMessage());
        e = e.getNextException();
      }
      return;
    } finally {
      try {
        callableStatement.close();
        connection.close();
      } catch (SQLException e) {
        LOG.error("SQLException while closing connection:"+ e.getMessage());
      }
    }

    // sleep till next roll up hour
    long waitTime = getTimeToSleep();
    LOG.debug("Sleeping for "+waitTime+"ms");
    try {
      Thread.sleep(waitTime);
    } catch (InterruptedException e) {
      LOG.warn("RollUp Service interrupted", e);
    }

    /*if (!stopAfterOneRun) {
      // sleep till next roll up hour
      long waitTime = getTimeToSleep();
      LOG.debug("Sleeping for "+waitTime+"ms");
      try {
        Thread.sleep(waitTime);
      } catch (InterruptedException e) {
        LOG.warn("RollUp Service interrupted", e);
      }
    }*/
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
