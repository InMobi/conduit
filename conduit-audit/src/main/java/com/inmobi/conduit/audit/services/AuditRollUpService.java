package com.inmobi.conduit.audit.services;

import com.inmobi.conduit.audit.AuditDBService;
import com.inmobi.conduit.audit.util.AuditDBConstants;
import com.inmobi.conduit.audit.util.AuditDBHelper;
import com.inmobi.messaging.ClientConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;

public abstract class AuditRollUpService extends AuditDBService {

  final protected int rollUpHourOfDay;
  final protected String masterTable;
  protected long intervalLength;
  private static final Log LOG = LogFactory.getLog(AuditRollUpService.class);

  public AuditRollUpService(ClientConfig config) {
    super(config);
    rollUpHourOfDay = config.getInteger(AuditDBConstants.ROLLUP_HOUR_KEY, 0);
    masterTable = config.getString(AuditDBConstants.MASTER_TABLE_NAME);
  }

  @Override
  public void stop() {
    isStop = true;
    // RollupService sleeps for a day between runs so have to interrupt the
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

  /**
   * public helper method to mark the checkpoint to the time at which to
   * start next run
   * @param toTime time to mark
   */
  public void mark(Long toTime) {
    LOG.info("Clearing the interrupted status of thread before marking");
    Thread.interrupted();
    try {
      rollupProvider.checkpoint(rollupChkPtKey, toTime.toString().getBytes());
      LOG.info("Marked checkpoint to the date at which to start next run:"
          + toTime);
    } catch (Exception e) {
      LOG.error("Marking checkpoint failed", e);
    }
  }

  @Override
  public void execute() {

    try {
      while (!isStop && !thread.isInterrupted()) {
        LOG.info("Starting new run");
        Connection connection = AuditDBHelper.getConnection(config);
        while (connection == null && !isStop) {
          LOG.info("Connection not initialized. Retry after 5 minutes");
          try {
            Thread.sleep(300000l);
          } catch (InterruptedException e) {
            LOG.error("Interrupted before connecting to db", e);
          }
          LOG.info("Retrying to establish connection.");
          if (!isStop) {
            connection = AuditDBHelper.getConnection(config);
          }
        }
        LOG.info("Connection initialized");

        try {
          if (!isStop) {
            executeRollup(connection);
          }
        } catch (SQLException e) {
          AuditDBHelper.logNextException("SQLException while rollup up " +
              "tables: No tables have been rolled up", e);
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
    } catch (Throwable th) {
      throw new RuntimeException(th);
    }
  }

  protected boolean createDailyTable(Connection connection) {
    if (!isStop) {
      Date fromDate = new Date();
      Date todate = AuditDBHelper.addDaysToCurrentDate(config.getInteger
          (AuditDBConstants.NUM_DAYS_AHEAD_TABLE_CREATION));
      LOG.info("Creating daily table from:" + fromDate + " till:" + todate);
      return createDailyTable(fromDate, todate, connection);
    }
    return false;
  }

  /**
   * public helper method to create daily table within the time range
   * [fromDate, toDate]
   * @param fromDate start date from which to create daily tables
   * @param todate end date till which to create daily tables
   * @param connection Connection to audit db
   * @return true if all daily tables have been created within this time
   * range else false
   */
  public boolean createDailyTable(Date fromDate, Date todate,
                                  Connection connection) {
    CallableStatement createDailyTableStmt = null;
    try {
      if (!isStop) {
        String statement = getCreateTableQuery();
        createDailyTableStmt = connection.prepareCall(statement);
        int addedToBatch = 0;
        while (!fromDate.after(todate) && !isStop) {
          String currentDateString = AuditDBHelper.DAY_CHK_FORMATTER.format(fromDate);
          String nextDayString = AuditDBHelper.DAY_CHK_FORMATTER.format
              (AuditDBHelper.addDaysToGivenDate(fromDate, 1));
          String dayTable = createMinuteTableName(config, fromDate);
          int index = 1;
          createDailyTableStmt.setString(index++, masterTable);
          createDailyTableStmt.setString(index++, dayTable);
          createDailyTableStmt.setString(index++, currentDateString);
          createDailyTableStmt.setString(index++, nextDayString);
          createDailyTableStmt.addBatch();
          LOG.debug("Daily table added to batch for day:" + currentDateString
              + " with table name as:" + dayTable + " and parent is :" +
              masterTable);
          addedToBatch++;
          fromDate = AuditDBHelper.addDaysToGivenDate(fromDate, 1);
        }
        if (!isStop) {
          LOG.info("Executing batch update for creating daily tables");
          int[] retVal = createDailyTableStmt.executeBatch();
          if (retVal.length != addedToBatch) {
            LOG.error("Mismatch in number of tables added to batch[" +
                addedToBatch + "] and rolledup tables[" + retVal.length + "]");
          }
          connection.commit();
        }
      }
    } catch (SQLException e) {
      AuditDBHelper.logNextException("SQLException while creating daily " +
          "table: No daily tables created for this run", e);
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

  private String getCreateTableQuery() {
    String query = "{call createDailyTable(?,?,?,?)}";
    return query;
  }

  private void sleepTillNextRun() {
    // sleep till next roll up hour
    long waitTime = getTimeToSleep();
    try {
      if (!isStop) {
        LOG.info("Sleeping for "+waitTime+"ms");
        Thread.sleep(waitTime);
      }
    } catch (InterruptedException e) {
      LOG.warn("RollUp Service interrupted", e);
    }
  }

  /**
   * public helper method to rollup daily tables within time range
   * [fromTime, toDate) i.e toDate table will not be rolled up. If rollup of
   * all tables is successful then toDate is returned and is the date at
   * which to start next rollup run.
   * @param fromTime start date of tables to be rolled up
   * @param toDate date of tables at which to stop roll up
   * @param connection Connection to audit db
   * @return date at which to checkpoint and start next rollup run
   * @throws SQLException
   */
  public Date rollupTables(Date fromTime, Date toDate,
                           Connection connection) throws SQLException {
    CallableStatement rollupStmt = null;
    Date currentDate = fromTime;
    try {
      if (!isStop) {
        String statement = getRollUpQuery();
        rollupStmt = connection.prepareCall(statement);
        LOG.info("Starting roll up of tables from:"+currentDate+" till:"+toDate);
        while (currentDate.before(toDate) && !isStop) {
          Date nextDay = AuditDBHelper.addDaysToGivenDate(currentDate, 1);
          String srcTable = getSourceTable(connection, currentDate);
          String destTable = createRolledTableNameForService(currentDate);
          Long firstMillisOfDay = AuditDBHelper.getFirstMilliOfDay(currentDate);
          Long firstMillisOfNextDay = AuditDBHelper.getFirstMilliOfDay(nextDay);
          int index = 1;
          rollupStmt.setString(index++, srcTable);
          rollupStmt.setString(index++, destTable);
          rollupStmt.setString(index++, masterTable);
          rollupStmt.setLong(index++, firstMillisOfDay);
          rollupStmt.setLong(index++, firstMillisOfNextDay);
          rollupStmt.setLong(index++, intervalLength);
          LOG.debug("Executing rollup of table for day:" + currentDate + " " +
              "and query:" + rollupStmt);
          try {
            rollupStmt.executeUpdate();
            LOG.debug("Rolled up table for day:" + currentDate);
          } catch (SQLException e) {
            AuditDBHelper.logNextException("SQLException while executing " +
                "rollup transaction for date" + currentDate, e);
            break;
          }
          currentDate = nextDay;
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

  protected abstract Date rollupTables(Connection connection) throws
      SQLException;

  protected abstract void executeRollup(Connection connection) throws
      SQLException;

  protected abstract String getSourceTable(Connection connection, Date date);

  public int getTillDays() {
    return tillDays;
  }
}
