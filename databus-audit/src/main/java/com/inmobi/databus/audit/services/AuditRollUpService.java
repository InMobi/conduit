package com.inmobi.databus.audit.services;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.inmobi.databus.FSCheckpointProvider;
import com.inmobi.databus.audit.AuditService;
import com.inmobi.databus.audit.util.AuditDBConstants;
import com.inmobi.databus.audit.util.AuditDBHelper;
import com.inmobi.messaging.ClientConfig;

public class AuditRollUpService extends AuditService {

  final private ClientConfig config;
  final private int rollUpHourOfDay;
  final private long intervalLength;
  final private String checkPointDir;
  private final String destnTableName, srcTableName;
  final private int tilldays;
  private static String ROLLUP_HOUR_KEY = "feeder.rollup.hour";
  private static String INTERVAL_LENGTH_KEY = "feeder.rollup.intervallength.millis";
  private static String CHECKPOINT_DIR_KEY = "feeder.rollup.checkpoint.dir";
  private static String CHECKPOINT_KEY = "feeder";
  private static String TILLDAYS_KEY = "feeder.rollup.tilldays";
  private static String MONTHLY_TABLE_KEY = "feeder.rollup.table.monthly";
  private static String DAILY_TABLE_KEY = "feeder.rollup.table.daily";
  private static final Log LOG = LogFactory.getLog(AuditRollUpService.class);
  public AuditRollUpService(ClientConfig config) {
    this.config = config;
    rollUpHourOfDay = config.getInteger(ROLLUP_HOUR_KEY, 0);
    intervalLength = config.getLong(INTERVAL_LENGTH_KEY, 3600000l);
    checkPointDir = config.getString(CHECKPOINT_DIR_KEY);
    tilldays = config.getInteger(TILLDAYS_KEY);
    destnTableName = config.getString(MONTHLY_TABLE_KEY);
    srcTableName = config.getString(DAILY_TABLE_KEY);
  }

  @Override
  public void stop() {
    // since DB handles transactions and all operations are done in 1 single
    // transaction so no need to implement stop

  }

  private Long getToTime() {
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.DAY_OF_MONTH, -tilldays);
    cal.set(Calendar.HOUR_OF_DAY, 23);
    cal.set(Calendar.MINUTE, 59);
    cal.set(Calendar.SECOND, 59);
    cal.set(Calendar.MILLISECOND, 59);
    return cal.getTimeInMillis();
  }

  private long getTimeToSleep(){
    Calendar cal =Calendar.getInstance();
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
    String query = "select rollup(?,?,?,?,?)";
    return query;
  }

  private String formatDate(Date date) {
    SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
    return formatter.format(date);
  }
  private String getDailyTableName(Date date) {
    return srcTableName + formatDate(date);
  }

  private String getMonthlyTableName(Date date) {
    return destnTableName + formatDate(date);
  }

  /*
   * Queries the daily table and finds the first entry ordered by timeinterval
   * Return the corresponding timeinterval
   */
  private Long getFirstTimeIntervalDailyTable(Connection connection)
      throws SQLException {
    String statement = "select timeinterval from " + srcTableName
        + " order by timeinterval limit 1";
    PreparedStatement preparedStatement = connection
        .prepareStatement(statement);
    preparedStatement.execute();
    Long result = 0l;
    ResultSet rs = preparedStatement.getResultSet();
    if (rs.next())
      result = rs.getLong("timeinterval");
    return result;
  }

  private Long getFromTime(Connection connection) throws SQLException {
    FSCheckpointProvider provider = new FSCheckpointProvider(checkPointDir);
    byte[] value = provider.read(CHECKPOINT_KEY);
    if (value == null)
      return getFirstTimeIntervalDailyTable(connection);
    return Long.parseLong(new String(value));
  }

  private void modifyContraints(Map<String, Long> tableConstraints,
      Connection connection) throws SQLException {
    PreparedStatement preparedstatement = null;
    String statement = "select modifyconstraint(?,?)";
    preparedstatement = connection.prepareStatement(statement);
    for (Entry<String, Long> tableConstraint : tableConstraints.entrySet()) {
      int index = 1;
      preparedstatement.setString(index++, tableConstraint.getKey());
      preparedstatement.setLong(index++, tableConstraint.getValue());
      preparedstatement.addBatch();
    }
    preparedstatement.executeBatch();
  }

  /*
   * Return date corresponding to first millisecond of month to which time
   * belongs
   */
  private Date getFirstMilliOfMonth(Long time) {
    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(time);
    cal.set(Calendar.DATE, 1);
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    return cal.getTime();
  }

  private Date getLastMilliOfMonth(Long time) {
    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(time);
    cal.set(Calendar.DATE, cal.getActualMaximum(Calendar.DATE));
    cal.set(Calendar.HOUR_OF_DAY, 23);
    cal.set(Calendar.MINUTE, 59);
    cal.set(Calendar.SECOND, 59);
    cal.set(Calendar.MILLISECOND, 59);
    return cal.getTime();
  }

  /**
   * Return the updated upper constraint for monthly table In case fromTime and
   * toTime belongs to different months there would be multiple monthly tables
   * which needs updation
   * 
   * @param fromTime
   * @param toTime
   * @return
   */
  private Map<String, Long> getTableConstraint(Long fromTime, Long toTime) {
    Date fromDate=getFirstMilliOfMonth(fromTime);
    Date toDate=getFirstMilliOfMonth(toTime);
    Calendar cal = Calendar.getInstance();
    Map<String,Long> tableConstraints= new HashMap<String, Long>();
    while(!fromDate.after(toDate)){
      Date lastDate = getLastMilliOfMonth(fromDate.getTime());
      if (toDate.after(lastDate)) {
        // insertion was across monthly tables and this constraint is for
        // older month's table
        tableConstraints.put(getMonthlyTableName(fromDate), lastDate.getTime());
      } else {
        // constraint for the current month's table
        tableConstraints.put(getMonthlyTableName(fromDate), toTime);
      }
      cal.setTime(fromDate);
      cal.add(Calendar.MONTH, 1);
      fromDate = cal.getTime();
    }
    return tableConstraints;
  }

  /**
   * Just remove the inheritance of the daily partition from the parent master
   * table Eventually same method would be extended to drop partition along with
   * NO Inherit
   * 
   * @param time
   * @throws SQLException
   */
  private void dropDailyTable(Date fromDate, Date toDate, Connection connection)
      throws SQLException {
    String statement = "alter table ?  no inherit "
        + srcTableName;
    PreparedStatement preparedstatement = connection
        .prepareStatement(statement);
    Calendar cal = Calendar.getInstance();
    while (toDate.after(fromDate)) {
      String tableName = getDailyTableName(fromDate);
      preparedstatement.setString(1, tableName);
      preparedstatement.addBatch();
      cal.setTime(fromDate);
      cal.add(Calendar.DATE, 1);
      fromDate = cal.getTime();
    }
    preparedstatement.executeBatch();
  }

  private void mark(Long fromTime, Long toTime, Connection connection)
      throws SQLException {
    FSCheckpointProvider provider = new FSCheckpointProvider(checkPointDir);
    provider.checkpoint(CHECKPOINT_KEY, toTime.toString().getBytes());
    Map<String, Long> tableConstraints = getTableConstraint(fromTime, toTime);
    modifyContraints(tableConstraints, connection);
    dropDailyTable(new Date(fromTime), new Date(toTime), connection);
    connection.commit();
  }
  @Override
  public void execute() {
    // wait till the roll up hour
    long waitTime = getTimeToSleep();
    try {
      Thread.sleep(waitTime);
    } catch (InterruptedException e) {
      LOG.warn("RollUp Service interrupted", e);
    }

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
    Long fromTime;
    try {
      fromTime = getFromTime(connection);
    } catch (SQLException e1) {
      LOG.error("Cannot find the from time", e1);
      return;
    }
    Long toTime = getToTime();
    // TODO create new destination partition table if doesn't exist and also
    // modify the corresponding trigger
    String statement = getRollUpQuery();
    PreparedStatement preparedstatement = null;
    try {
      preparedstatement = connection.prepareStatement(statement);
      int index = 1;
      preparedstatement.setString(index++, destnTableName);
      preparedstatement.setString(index++, srcTableName);
      preparedstatement.setLong(index++, fromTime);
      preparedstatement.setLong(index++, toTime);
      preparedstatement.setLong(index++, intervalLength);
      LOG.info("Rollup query is " + preparedstatement.toString());
      preparedstatement.executeQuery();
      mark(fromTime, toTime, connection);
    } catch (SQLException e) {
      LOG.error("Error while rollup", e);
      return;
    }
  }

  @Override
  public String getServiceName() {
    return "RollUpService";
  }

}
