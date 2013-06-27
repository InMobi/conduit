package com.inmobi.messaging.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.inmobi.databus.audit.LatencyColumns;
import com.inmobi.databus.audit.Tuple;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.consumer.audit.Column;
import com.inmobi.messaging.consumer.audit.Filter;
import com.inmobi.messaging.consumer.audit.GroupBy;

public class AuditDBHelper {

  private static final Log LOG = LogFactory.getLog(AuditDBHelper.class);
  private final ClientConfig config;

  public AuditDBHelper(ClientConfig config) {
    this.config = config;
  }

  public static Connection getConnection(String driverName, String url,
                                          String username, String password) {
    try {
      Class.forName(driverName).newInstance();
    } catch (Exception e) {
      LOG.error("Exception while registering jdbc driver ", e);
    }
    Connection connection = null;
    try {
      connection = DriverManager.getConnection(url, username, password);
      connection.setAutoCommit(false);
    } catch (SQLException e) {
      LOG.error("Exception while creating db connection ", e);
    }
    return connection;
  }

  public boolean update(Set<Tuple> tupleSet) {

    LOG.info("Connecting to DB ...");
    Connection connection =
        getConnection(config.getString(AuditDBConstants.JDBC_DRIVER_CLASS_NAME),
            config.getString(AuditDBConstants.DB_URL),
            config.getString(AuditDBConstants.DB_USERNAME),
            config.getString(AuditDBConstants.DB_PASSWORD));
    if (connection == null) {
      LOG.error("Connection not initialized returning ...");
      return false;
    }
    LOG.info("Connected to DB");

    boolean isUpdate = false, isInsert = false;
    ResultSet rs = null;
    String selectstatement = getSelectStmtForUpdation();
    String insertStatement = getInsertStmtForUpdation();
    String updateStatement = getUpdateStmtForUpdation();
    PreparedStatement selectPreparedStatement = null, insertPreparedStatement =
        null, updatePreparedStatement = null;
    try {
      selectPreparedStatement = connection.prepareStatement(selectstatement);
      insertPreparedStatement = connection.prepareStatement(insertStatement);
      updatePreparedStatement = connection.prepareStatement(updateStatement);
      for (Tuple tuple : tupleSet) {
        rs = executeSelectStmtUpdation(selectPreparedStatement, tuple);
        if (rs.next()) {
          if (!addToUpdateStatementBatch(updatePreparedStatement, tuple, rs))
            return false;
          isUpdate = true;
        } else {
          if (!addToInsertStatementBatch(insertPreparedStatement, tuple))
            return false;
          isInsert = true;
        }
      }
      if (isUpdate)
        updatePreparedStatement.executeBatch();
      if (isInsert)
        insertPreparedStatement.executeBatch();
      connection.commit();
    } catch (SQLException e) {
      LOG.error("SQLException thrown ", e);
      return false;
    } finally {
      try {
        if (rs != null) {
          rs.close();
        }
        selectPreparedStatement.close();
        insertPreparedStatement.close();
        updatePreparedStatement.close();
        connection.close();
      } catch (SQLException e) {
        LOG.warn("Exception while closing ", e);
      }
    }
    return true;
  }

  private static ResultSet executeSelectStmtUpdation(
      PreparedStatement selectPreparedStatement, Tuple tuple) {
    int i = 1;
    ResultSet rs;
    try {
      selectPreparedStatement.setLong(i++, tuple.getTimestamp().getTime());
      selectPreparedStatement.setString(i++, tuple.getHostname());
      selectPreparedStatement.setString(i++, tuple.getTopic());
      selectPreparedStatement.setString(i++, tuple.getTier());
      selectPreparedStatement.setString(i++, tuple.getCluster());
      rs = selectPreparedStatement.executeQuery();
    } catch (SQLException e) {
      LOG.error("Exception encountered ", e);
      return null;
    }
    return rs;
  }

  private static String getUpdateStmtForUpdation() {
    String setString = "";
    for (LatencyColumns columns : LatencyColumns.values()) {
      setString += ", " + columns.toString() + " = ?";
    }
    String updateStatement = "update " + AuditDBConstants.TABLE_NAME + " set " +
        "" + AuditDBConstants.SENT + " = ?" + setString + " where " +
        Column.HOSTNAME + " = ? and " + Column.TIER + " = ? and " +
        Column.TOPIC +
        " = ? and " + Column.CLUSTER + " = ? and " +
        AuditDBConstants.TIMESTAMP + " = ? ";
    LOG.debug("Update statement: " + updateStatement);
    return updateStatement;
  }

  private static String getInsertStmtForUpdation() {
    String columnString = "", columnNames = "";
    for (LatencyColumns column : LatencyColumns.values()) {
      columnNames += column.toString() + ", ";
      columnString += "?, ";
    }
    String insertStatement =
        "insert into " + AuditDBConstants.TABLE_NAME + " (" + columnNames +
            AuditDBConstants.TIMESTAMP + "," + Column.HOSTNAME +
            ", " + Column.TIER + ", " + Column.TOPIC +
            ", " + Column.CLUSTER + ", " + AuditDBConstants.SENT + ") values " +
            "(" + columnString + "?, ?, ?, ?, ?, ?)";
    LOG.debug("Insert statement: " + insertStatement);
    return insertStatement;
  }

  public static String getSelectStmtForUpdation() {
    String selectstatement =
        "select * from " + AuditDBConstants.TABLE_NAME + " where " +
            AuditDBConstants.TIMESTAMP + " = ? and " + Column.HOSTNAME + " = " +
            "? and " + Column.TOPIC + " = ? and " + Column.TIER + "" +
            " = ? and " + Column.CLUSTER + " = ?";
    LOG.debug("Select statement: " + selectstatement);
    return selectstatement;
  }

  private static boolean addToInsertStatementBatch(
      PreparedStatement insertPreparedStatement, Tuple tuple) {
    try {
      LOG.debug("Inserting tuple in DB " + tuple);
      int index = 1;
      Map<LatencyColumns, Long> latencyCountMap = tuple.getLatencyCountMap();
      for (LatencyColumns latencyColumn : LatencyColumns.values()) {
        Long count = latencyCountMap.get(latencyColumn);
        if (count == null)
          count = 0l;
        insertPreparedStatement.setLong(index++, count);
      }
      insertPreparedStatement.setLong(index++, tuple.getTimestamp().getTime());
      insertPreparedStatement.setString(index++, tuple.getHostname());
      insertPreparedStatement.setString(index++, tuple.getTier());
      insertPreparedStatement.setString(index++, tuple.getTopic());
      insertPreparedStatement.setString(index++, tuple.getCluster());
      insertPreparedStatement.setLong(index++, tuple.getSent());
      LOG.debug(
          "Insert prepared statement : " + insertPreparedStatement.toString());
      insertPreparedStatement.addBatch();
    } catch (SQLException e) {
      LOG.error("Exception thrown while adding to insert statement batch", e);
      return false;
    }
    return true;
  }

  private static boolean addToUpdateStatementBatch(
      PreparedStatement updatePreparedStatement, Tuple tuple, ResultSet rs) {
    try {
      LOG.debug("Updating tuple in DB:" + tuple);
      Map<LatencyColumns, Long> latencyCountMap =
          new HashMap<LatencyColumns, Long>();
      latencyCountMap.putAll(tuple.getLatencyCountMap());
      for (LatencyColumns latencyColumn : LatencyColumns.values()) {
        Long currentVal = latencyCountMap.get(latencyColumn);
        Long prevVal = rs.getLong(latencyColumn.toString());
        if (currentVal == null)
          currentVal = 0l;
        if (prevVal == null)
          prevVal = 0l;
        Long count = currentVal + prevVal;
        latencyCountMap.put(latencyColumn, count);
      }
      Long sent = tuple.getSent() + rs.getLong(AuditDBConstants.SENT);
      int index = 1;
      updatePreparedStatement.setLong(index++, sent);
      for (LatencyColumns latencyColumn : LatencyColumns.values()) {
        updatePreparedStatement
            .setLong(index++, latencyCountMap.get(latencyColumn));
      }
      updatePreparedStatement.setString(index++, tuple.getHostname());
      updatePreparedStatement.setString(index++, tuple.getTier());
      updatePreparedStatement.setString(index++, tuple.getTopic());
      updatePreparedStatement.setString(index++, tuple.getCluster());
      updatePreparedStatement.setLong(index++, tuple.getTimestamp().getTime());
      LOG.debug(
          "Update prepared statement : " + updatePreparedStatement.toString());
      updatePreparedStatement.addBatch();
    } catch (SQLException e) {
      LOG.error("Exception thrown while adding to batch of update statement",
          e);
      return false;
    }
    return true;
  }



  public Set<Tuple> retrieve(Date toDate, Date fromDate, Filter filter,
      GroupBy groupBy) {
    LOG.debug("Retrieving from db  from-time :" + fromDate + " to-date :" +
 ":"
        + toDate + " filter :" + filter.toString());
    Set<Tuple> tupleSet = new HashSet<Tuple>();

    LOG.info("Connecting to DB ...");
    Connection connection =
        getConnection(config.getString(AuditDBConstants.JDBC_DRIVER_CLASS_NAME),
            config.getString(AuditDBConstants.DB_URL),
            config.getString(AuditDBConstants.DB_USERNAME),
            config.getString(AuditDBConstants.DB_PASSWORD));
    if (connection == null) {
      LOG.error("Connection not initialized returning ...");
      return null;
    }
    LOG.info("Connected to DB");
    ResultSet rs = null;
    String statement = getSelectStmtForRetrieve(filter, groupBy);
    LOG.debug("Select statement :" + statement);
    PreparedStatement preparedstatement = null;
    try {
      preparedstatement = connection.prepareStatement(statement);
      int index = 1;
      preparedstatement.setLong(index++, fromDate.getTime());
      preparedstatement.setLong(index++, toDate.getTime());
      if (filter.getFilters() != null) {
      for (Column column : Column.values()) {
        String value = filter.getFilters().get(column);
        if (value != null && !value.isEmpty()) {
          preparedstatement.setString(index++, value);
        }
      }
      }
      LOG.debug("Prepared statement is " + preparedstatement.toString());
      rs = preparedstatement.executeQuery();
      while (rs.next()) {
        Tuple tuple = createNewTuple(rs, groupBy);
        if (tuple == null) {
          LOG.error("Returned null tuple..returning");
          return null;
        }
        tupleSet.add(tuple);
      }
      connection.commit();
    } catch (SQLException e) {
      LOG.error("SQLException encountered", e);
    } finally {
      try {
        if (rs != null)
          rs.close();
        if (preparedstatement != null)
          preparedstatement.close();
        connection.close();
      } catch (SQLException e) {
        LOG.warn("Exception while closing ", e);
      }
    }
    return tupleSet;
  }

  private static Tuple createNewTuple(ResultSet rs, GroupBy groupBy) {
    Tuple tuple;
    try {
      Map<Column, String> columnValuesInTuple = new HashMap<Column, String>();
      for (Column column : Column.values()) {
        if (groupBy.getGroupByColumns().contains(column))
          columnValuesInTuple.put(column, rs.getString(column.toString()));
      }
      Map<LatencyColumns, Long> latencyCountMap =
          new HashMap<LatencyColumns, Long>();
      for (LatencyColumns latencyColumn : LatencyColumns.values()) {
        latencyCountMap
            .put(latencyColumn, rs.getLong(latencyColumn.toString()));
      }
      tuple = new Tuple(columnValuesInTuple.get(Column.HOSTNAME),
          columnValuesInTuple.get(Column.TIER),
          columnValuesInTuple.get(Column.CLUSTER), null,
          columnValuesInTuple.get(Column.TOPIC), latencyCountMap,
          rs.getLong(AuditDBConstants.SENT));
    } catch (SQLException e) {
      LOG.error("Exception thrown while creating new tuple ", e);
      return null;
    }
    return tuple;
  }

  private static String getSelectStmtForRetrieve(Filter filter,
                                                 GroupBy groupBy) {
    String sumString = "", whereString = "", groupByString = "";
    for (LatencyColumns latencyColumn : LatencyColumns.values()) {
      sumString += ", Sum(" + latencyColumn.toString() + ") as " +
          latencyColumn.toString();
    }
    if (filter.getFilters() != null) {
    for (Column column : Column.values()) {
      String value = filter.getFilters().get(column);
      if (value != null && !value.isEmpty()) {
        whereString += " and " + column.toString() +" = ?";

      }
    }
    }
    for (Column column : groupBy.getGroupByColumns()) {
      if (!groupByString.isEmpty()) {
        groupByString += ", " + column.toString();
      } else {
        groupByString += column.toString();
      }
    }
    String statement =
        "select " + groupByString + ", Sum(" + AuditDBConstants.SENT + ") as " +
            AuditDBConstants.SENT + sumString + " from " +
            AuditDBConstants.TABLE_NAME + " where " +
            AuditDBConstants.TIMESTAMP + " >= ? and" +
            " " + AuditDBConstants.TIMESTAMP + " < ? " + whereString + " " +
            "group by " + groupByString;
    LOG.debug("Select statement " + statement);
    return statement;
  }
}
