package com.inmobi.databus.visualization.server.util;

import com.google.protobuf.gwt.server.ServerJsonStreamFactory;
import com.inmobi.databus.audit.Column;
import com.inmobi.databus.audit.LatencyColumns;
import com.inmobi.databus.audit.Tuple;
import com.inmobi.databus.audit.util.AuditDBConstants;
import com.inmobi.databus.audit.util.AuditDBHelper;
import com.inmobi.databus.visualization.server.MessageStats;
import com.inmobi.databus.visualization.server.Node;
import com.inmobi.databus.visualization.server.NodeKey;
import com.inmobi.databus.visualization.shared.RequestResponse;
import com.inmobi.messaging.ClientConfig;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Assert;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.Date;

public class TestUtil {

  private static Logger LOG = Logger.getLogger(TestUtil.class);

  public static List<Tuple> tupleList = new ArrayList<Tuple>();
  static Date currentDate;

  static {
    currentDate = new Date();
    Calendar currentCal = Calendar.getInstance();
    currentCal.setTime(currentDate);
    currentCal.add(Calendar.MINUTE, 2);
    Date currPlusTwo = currentCal.getTime();
    currentCal.add(Calendar.MINUTE, 1);
    Date currPlusThree = currentCal.getTime();
    int index = 0;
    Map<LatencyColumns, Long> latencyValueMap = new HashMap<LatencyColumns,
        Long>();
    for (LatencyColumns latencyColumn : LatencyColumns.values()) {
      latencyValueMap.put(latencyColumn, 0l);
    }
    latencyValueMap.put(LatencyColumns.C0, 1000l);
    latencyValueMap.put(LatencyColumns.C1, 800l);
    latencyValueMap.put(LatencyColumns.C2, 700l);
    Tuple tuple = new Tuple("testHost2", "publisher", "testCluster1",
        currentDate, "testTopic2", latencyValueMap, 0l);
    tupleList.add(index++, tuple);
    tuple = new Tuple("testHost2", "agent", "testCluster1", currentDate,
        "testTopic2", latencyValueMap, 2500l);
    tupleList.add(index++, tuple);
    tuple = new Tuple("testHost1", "collector", "testCluster1", currPlusTwo,
        "testTopic2", latencyValueMap, 2500l);
    tupleList.add(index++, tuple);
    tuple = new Tuple("testHost2", "collector", "testCluster1", currPlusTwo,
        "testTopic1", latencyValueMap, 2500l);
    tupleList.add(index++, tuple);
    tuple = new Tuple("testHost1", "hdfs", "testCluster1", currPlusThree,
        "testTopic2", latencyValueMap, 0l);
    tupleList.add(index++, tuple);
    tuple = new Tuple("testHost2", "hdfs", "testCluster1", currPlusThree,
        "testTopic1", latencyValueMap, 0l);
    tupleList.add(index++, tuple);
    Map<LatencyColumns, Long> latencyValueMap1 = new HashMap<LatencyColumns,
        Long>();
    for (LatencyColumns latencyColumn : LatencyColumns.values()) {
      latencyValueMap1.put(latencyColumn, 0l);
    }
    latencyValueMap1.put(LatencyColumns.C0, 2000l);
    latencyValueMap1.put(LatencyColumns.C1, 1600l);
    latencyValueMap1.put(LatencyColumns.C2, 1400l);
    tuple = new Tuple("testHost1", "publisher", "testCluster1",
        currentDate, "testTopic1", latencyValueMap1, 0l);
    tupleList.add(index++, tuple);
    tuple = new Tuple("testHost1", "agent", "testCluster1", currentDate,
        "testTopic1", latencyValueMap1, 5000l);
    tupleList.add(index++, tuple);
    Map<LatencyColumns, Long> latencyValueMap2 = new HashMap<LatencyColumns,
        Long>();
    for (LatencyColumns latencyColumn : LatencyColumns.values()) {
      latencyValueMap2.put(latencyColumn, 0l);
    }
    latencyValueMap2.put(LatencyColumns.C5, 1500l);
    latencyValueMap2.put(LatencyColumns.C10, 1000l);
    tuple = new Tuple("testHost1", "collector", "testCluster1", currPlusTwo,
        "testTopic1", latencyValueMap2, 2500l);
    tupleList.add(index++, tuple);
    tuple = new Tuple("testHost1", "hdfs", "testCluster1", currPlusThree,
        "testTopic1", latencyValueMap2, 0l);
    tupleList.add(index++, tuple);
  }

  public static Connection setupDB() throws Exception {
    ClientConfig config = ClientConfig.loadFromClasspath(UtilConstants
        .CONF_FILE);
    String driverName = config.getString(UtilConstants.JDBC_DRIVER_CLASS_NAME);
    String url = config.getString(UtilConstants.DB_URL);
    String username = config.getString(UtilConstants.DB_USERNAME);
    String password = config.getString(UtilConstants.DB_PASSWORD);
    String table = config.getString(UtilConstants.TABLE_NAME);

    Connection connection = getConnection(driverName, url, username, password);
    dropTable(connection, table);
    createTable(connection, table);
    String insertStatement = getInsertStatement(table);
    PreparedStatement preparedStatement = connection.prepareStatement
        (insertStatement);
    AuditDBHelper dbHelper = new AuditDBHelper(config);
    Assert.assertTrue(dbHelper.update(new HashSet<Tuple>(tupleList)));
    ResultSet rs = connection.prepareStatement("select * from "+table+";")
        .executeQuery();
    int ki = rs.getFetchSize();
    while (rs.next()) {
      System.out.println(rs.getString(1));
    }
    /*
    for (Tuple tuple : tupleList) {
      int index = 1;
      Map<LatencyColumns, Long> latencyCountMap = tuple.getLatencyCountMap();
      for (LatencyColumns latencyColumn : LatencyColumns.values()) {
        Long count = latencyCountMap.get(latencyColumn);
        if (count == null)
          count = 0l;
        preparedStatement.setLong(index++, count);
      }
      preparedStatement.setLong(index++, tuple.getTimestamp().getTime());
      preparedStatement.setString(index++, tuple.getHostname());
      preparedStatement.setString(index++, tuple.getTier());
      preparedStatement.setString(index++, tuple.getTopic());
      preparedStatement.setString(index++, tuple.getCluster());
      preparedStatement.setLong(index++, tuple.getSent());
      preparedStatement.addBatch();
    }
    preparedStatement.executeBatch();
    connection.commit();
    preparedStatement.close();*/
    return connection;
  }

  private static String getInsertStatement(String table) {
    String columnString = "", columnNames = "";
    for (LatencyColumns column : LatencyColumns.values()) {
      columnNames += column.toString() + ", ";
      columnString += "?, ";
    }
    String insertStatement = "insert into " + table + " (" + columnNames
        + AuditDBConstants.TIMESTAMP + "," + Column.HOSTNAME + ", "
        + Column.TIER + ", " + Column.TOPIC + ", " + Column.CLUSTER + ", "
        + AuditDBConstants.SENT + ") values " + "(" + columnString
        + "?, ?, ?, ?, ?, ?)";
    return insertStatement;
  }

  private static Connection getConnection(String driverName, String url,
                                          String username,
                                          String password) {
    LOG.debug("Connecting to Test DB");
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
    Assert.assertTrue(connection != null);
    LOG.debug("Connected to Test DB");
    return connection;
  }

  public static boolean createTable(Connection connection,
                                  String table) throws SQLException {
    String createTable = "CREATE TABLE "+ table + UtilConstants.columns;
    boolean isCreated = connection.prepareStatement(createTable).execute();
    if (isCreated)
      LOG.debug("Created table "+table);
    else
      LOG.debug("Failed to create table "+table);
    return isCreated;
  }

  public static boolean dropTable(Connection connection,
                               String table) throws SQLException {
    String dropTable = "DROP TABLE IF EXISTS " + table + ";";
    boolean isDropped = connection.prepareStatement(dropTable).execute();
    if (isDropped)
      LOG.debug("Dropped table "+table);
    else
      LOG.debug("Failed to drop table "+table);
    return isDropped;
  }

  public static void shutDownDB(Connection connection) {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }

  public static Tuple getTestTuple() {
    return tupleList.get(0);
  }

  public static NodeKey getNodeKeyOfTuple(Tuple tuple) {
    return new NodeKey(tuple.getHostname(), tuple.getCluster(),
        tuple.getTier());
  }
  public static List<Tuple> getTuplesWithSameNodeKeyDiffTopic() {
    List<Tuple> tupleList1 = new ArrayList<Tuple>();
    Tuple tuple1 = tupleList.get(2);
    Tuple tuple2 = tupleList.get(8);
    Assert.assertEquals(getNodeKeyOfTuple(tuple1), getNodeKeyOfTuple(tuple2));
    Assert.assertFalse(tuple1.getTopic().equalsIgnoreCase(tuple2.getTopic()));
    tupleList1.add(0, tuple1);
    tupleList1.add(1, tuple2);
    return tupleList1;
  }

  public static Tuple getHdfsTestTuple() {
    return tupleList.get(4);
  }

  public static NodeKey getNodeKeyOfHdfsOrVipTuple(Tuple tuple) {
    return new NodeKey(tuple.getCluster(), tuple.getCluster(),
        tuple.getTier());
  }

  public static List<String> getStreamsListFromResponse(String json) {
    RequestResponse.Response response = null;
    try {
      response = RequestResponse.Response.newBuilder().readFrom(
          ServerJsonStreamFactory.getInstance()
              .createNewStreamFromJson(json)).build();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return response.getLoadMainPanelResponse().getStreamList();
  }

  public static List<String> getClustersListFromResponse(String json) {
    RequestResponse.Response response = null;
    try {
      response = RequestResponse.Response.newBuilder().readFrom(
          ServerJsonStreamFactory.getInstance()
              .createNewStreamFromJson(json)).build();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return response.getLoadMainPanelResponse().getClusterList();
  }

  public static String getFilterStringFromParameters(String start, String end,
                                              String stream, String cluster) {
    RequestResponse.Request request;
    request = RequestResponse.Request.newBuilder().setGraphDataRequest(
        RequestResponse.GraphDataRequest.newBuilder().setStartTime(start)
            .setEndTime(end).setStream(stream).setColo(cluster).build())
        .build();
    return ServerJsonStreamFactory.getInstance().serializeMessage(request);
  }

  public static List<Tuple> getCollectorTuples() {
    List<Tuple> tupleList1 = new ArrayList<Tuple>();
    tupleList1.add(0, tupleList.get(2));
    tupleList1.add(1, tupleList.get(3));
    tupleList1.add(2, tupleList.get(8));
    return tupleList1;
  }

  public static Tuple getVipTuple() {
    return new Tuple("testCluster1", "VIP", "testCluster1",
        currentDate, null);
  }

  public static Date getCurrentDate() {
    return currentDate;
  }

  public static Date incrementDate(Date date, int i) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    calendar.add(Calendar.MINUTE, i);
    return calendar.getTime();
  }

  public static List<Node> getNodeListFromResponse(String dataString) {
    List<Node> nodeList = new ArrayList<Node>();
    RequestResponse.Response response = null;
    try {
      response = RequestResponse.Response.newBuilder().readFrom(
          ServerJsonStreamFactory.getInstance()
              .createNewStreamFromJson(dataString)).build();
    } catch (IOException e) {
      e.printStackTrace();
    }
    String jsonString = response.getGraphDataResponse().getJsonString();
    try {
      JSONObject dataObject = new JSONObject(jsonString);
      JSONArray nodeArray = dataObject.getJSONArray("nodes");
      for ( int i = 0; i < nodeArray.length(); i++) {
        JSONObject nodeObj = nodeArray.getJSONObject(i);
        Node node = new Node(nodeObj.getString("name"),
            nodeObj.getString("cluster"), nodeObj.getString("tier"));
        node.setAggregateMessagesReceived(nodeObj.getLong("aggregatereceived"));
        node.setAggregateMessagesSent(nodeObj.getLong("aggregatesent"));
        List<MessageStats> receivedList = new ArrayList<MessageStats>();
        JSONArray receivedArray = nodeObj.getJSONArray
            ("receivedtopicStatsList");
        for (int j = 0; j < receivedArray.length(); j++) {
          JSONObject messageObj = receivedArray.getJSONObject(j);
          MessageStats messageStats = new MessageStats(messageObj.getString
              ("topic"), messageObj.getLong("messages"),
              messageObj.getString("hostname"));
          receivedList.add(messageStats);
        }
        node.setReceivedMessagesList(receivedList);
        List<MessageStats> sentList = new ArrayList<MessageStats>();
        JSONArray sentArray = nodeObj.getJSONArray
            ("senttopicStatsList");
        for (int j = 0; j < sentArray.length(); j++) {
          JSONObject messageObj = sentArray.getJSONObject(j);
          MessageStats messageStats = new MessageStats(messageObj.getString
              ("topic"), messageObj.getLong("messages"),
              messageObj.getString("hostname"));
          sentList.add(messageStats);
        }
        node.setSentMessagesList(sentList);
        if ((node.getTier().equalsIgnoreCase("merge") ||
            node.getTier().equalsIgnoreCase("mirror"))) {
          JSONArray sourceArray = nodeObj.getJSONArray("source");
          Set<String> sourceSet = new HashSet<String>();
          for (int j = 0; j < sourceArray.length(); j++) {
            sourceSet.add(sourceArray.getString(j));
          }
          node.setSourceList(sourceSet);
        }
        JSONArray percentileLatencyArray = nodeObj.getJSONArray
            ("overallLatency");
        Map<Float, Integer> percentileMap = new HashMap<Float, Integer>();
        for (int j=0; j < percentileLatencyArray.length(); j++) {
          JSONObject obj = percentileLatencyArray.getJSONObject(j);
          percentileMap.put((Float) obj.get("percentile"), obj.getInt("latency"));
        }
        node.setPercentileMap(percentileMap);
        JSONArray topicPercentileArray = nodeObj.getJSONArray("topicLatency");
        for (int j = 0; j < topicPercentileArray.length(); j++) {
          JSONObject topicMapObj = topicPercentileArray.getJSONObject(j);
          String topic = topicMapObj.getString("topic");
          JSONArray topicMapArray = topicMapObj.getJSONArray
              ("percentileLatencyList");
          Map<Float, Integer> topicPercentileMap = new HashMap<Float, Integer>();
          for (int k=0; k< topicMapArray.length(); k++) {
            JSONObject percentileLatencyObj = topicMapArray.getJSONObject(k);
            topicPercentileMap.put((Float) percentileLatencyObj.get("percentile"),
                percentileLatencyObj.getInt("latency"));
          }
          node.addToTopicPercentileMap(topic, topicPercentileMap);
        }
        nodeList.add(node);
      }
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return nodeList;
  }
}
