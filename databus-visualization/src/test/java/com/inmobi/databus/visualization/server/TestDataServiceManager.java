package com.inmobi.databus.visualization.server;

import com.inmobi.databus.audit.LatencyColumns;
import com.inmobi.databus.audit.Tuple;
import com.inmobi.databus.visualization.server.util.TestUtil;
import junit.framework.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.*;

public class TestDataServiceManager {
  DataServiceManagerTest serviceManager;
  SimpleDateFormat auditDateFormatter = new SimpleDateFormat
      ("dd-MM-yyyy-HH:mm");

  @Test
  public void testAuditStreamNotAdded() {
    serviceManager = DataServiceManagerTest.get("" +
        "./src/test/resources/xmlfolder2",
        "./src/test/resources/visualization2.properties", null);
    String response = serviceManager.getStreamAndClusterList();
    List<String> streamList = TestUtil.getStreamsListFromResponse(response);
    Assert.assertFalse(streamList.contains(ServerConstants.AUDIT_STREAM));
  }

  @Test
  public void testMultipleXmls() {
    serviceManager = DataServiceManagerTest.get("" +
        "./src/test/resources/xmlfolder1",
        "./src/test/resources/visualization2.properties", null);
    assert serviceManager.getDataBusConfig().size() == 2;
    String response = serviceManager.getStreamAndClusterList();
    List<String> streamList = TestUtil.getStreamsListFromResponse(response);
    List<String> clusterList = TestUtil.getClustersListFromResponse(response);
    assert streamList.size() == 5;
    assert clusterList.size() == 6;
    assert streamList.get(0).equalsIgnoreCase("All");
    assert streamList.get(1).equalsIgnoreCase("vistest1");
    assert streamList.get(2).equalsIgnoreCase("vistest2");
    assert streamList.get(3).equalsIgnoreCase("vistest3");
    assert streamList.get(4).equalsIgnoreCase("vistest4");
    assert clusterList.get(0).equalsIgnoreCase("All");
    assert clusterList.get(1).equalsIgnoreCase("viscluster1");
    assert clusterList.get(2).equalsIgnoreCase("viscluster2");
    assert clusterList.get(3).equalsIgnoreCase("viscluster3");
    assert clusterList.get(4).equalsIgnoreCase("viscluster4");
    assert clusterList.get(5).equalsIgnoreCase("viscluster5");
  }

  @Test
  public void testVipNodeStreamsAddedCorrect() {
    serviceManager = DataServiceManagerTest.get("" +
        "./src/test/resources/xmlfolder2",
        "./src/test/resources/visualization2.properties", null);
    Set<Float> percentileSet = new HashSet<Float>();
    percentileSet.add(99.9f);
    Map<NodeKey, Node> nodeMap = new HashMap<NodeKey, Node>();
    List<Tuple> collectorTuples = TestUtil.getCollectorTuples();
    Map<Float, Integer> tuplePercentileMap1 = new HashMap<Float, Integer>();
    tuplePercentileMap1.put(99.9f, 2);
    serviceManager.createNode(collectorTuples.get(0), nodeMap, percentileSet,
        tuplePercentileMap1);
    Map<Float, Integer> tuplePercentileMap2 = new HashMap<Float, Integer>();
    tuplePercentileMap2.put(99.9f, 2);
    serviceManager.createNode(collectorTuples.get(1), nodeMap, percentileSet,
        tuplePercentileMap2);
    Map<Float, Integer> tuplePercentileMap3 = new HashMap<Float, Integer>();
    tuplePercentileMap3.put(99.9f, 2);
    serviceManager.createNode(collectorTuples.get(2), nodeMap, percentileSet,
        tuplePercentileMap3);
    serviceManager.addVIPNodesToNodesList(nodeMap, percentileSet);
    Assert.assertEquals(3, nodeMap.size());
    Tuple vipTuple = TestUtil.getVipTuple();
    NodeKey vipKey = TestUtil.getNodeKeyOfHdfsOrVipTuple(vipTuple);
    Assert.assertNotNull(nodeMap.get(vipKey));
    Node vipNode = nodeMap.get(vipKey);
    Assert.assertEquals(7500l, vipNode.getAggregateMessagesReceived().longValue());
    Assert.assertEquals(7500l, vipNode.getAggregateMessagesSent().longValue());
    Assert.assertEquals(2, vipNode.getReceivedMessagesList().size());
    Assert.assertEquals(2, vipNode.getSentMessagesList().size());
    MessageStats topic1Stat = new MessageStats(collectorTuples.get(0)
        .getTopic(), 5000l, null);
    MessageStats topic2stat = new MessageStats(collectorTuples.get(1)
        .getTopic(), 2500l, null);
    Assert.assertTrue(vipNode.getReceivedMessagesList().contains(topic1Stat));
    Assert.assertTrue(vipNode.getReceivedMessagesList().contains(topic2stat));
    Assert.assertTrue(vipNode.getSentMessagesList().contains(topic2stat));
    Assert.assertTrue(vipNode.getSentMessagesList().contains(topic1Stat));
  }

  @Test
  public void testNewNodeCreatedFromTuple() {
    serviceManager = DataServiceManagerTest.get("" +
        "./src/test/resources/xmlfolder2",
        "./src/test/resources/visualization2.properties", null);
    Set<Float> percentileSet = new HashSet<Float>();
    percentileSet.add(99.9f);
    Map<NodeKey, Node> nodeMap = new HashMap<NodeKey, Node>();
    Map<Float, Integer> tuplePercentileMap = new HashMap<Float, Integer>();
    tuplePercentileMap.put(99.9f, 2);
    Tuple tuple = TestUtil.getTestTuple();
    NodeKey key = TestUtil.getNodeKeyOfTuple(tuple);
    serviceManager.createNode(tuple, nodeMap, percentileSet,
        tuplePercentileMap);
    assert nodeMap.size() == 1;
    Assert.assertNotNull(nodeMap.get(key));
    Node node = nodeMap.get(key);
    Assert.assertEquals(2500l, node.getAggregateMessagesReceived().longValue());
    Assert.assertEquals(0l, node.getAggregateMessagesSent().longValue());
    Assert.assertNotNull(node.getReceivedMessagesList());
    Assert.assertNotNull(node.getSentMessagesList());
    Assert.assertEquals(1, node.getReceivedMessagesList().size());
    Assert.assertEquals(1, node.getSentMessagesList().size());
    MessageStats rMessageStat = new MessageStats(tuple.getTopic(), 2500l, null);
    MessageStats sMessageStat = new MessageStats(tuple.getTopic(), 0l, null);
    Assert.assertEquals(node.getReceivedMessagesList().get(0), rMessageStat);
    Assert.assertEquals(node.getSentMessagesList().get(0), sMessageStat);
    Assert.assertEquals(1000l, node.getPerTopicCountMap().get(tuple.getTopic
        ()).get(LatencyColumns.C0).longValue());
    Assert.assertEquals(800l, node.getPerTopicCountMap().get(tuple.getTopic
        ()).get(LatencyColumns.C1).longValue());
    Assert.assertEquals(700l, node.getPerTopicCountMap().get(tuple.getTopic
        ()).get(LatencyColumns.C2).longValue());
    Assert.assertEquals(2, node.getPerTopicPercentileMap().get(tuple.getTopic
        ()).get(99.9f).intValue());
  }

  @Test
  public void testNodeUpdatedWithNewTupleInfoOfDiffTopic() {
    serviceManager = DataServiceManagerTest.get("" +
        "./src/test/resources/xmlfolder2",
        "./src/test/resources/visualization2.properties", null);
    List<Tuple> tupleList = TestUtil.getTuplesWithSameNodeKeyDiffTopic();
    Set<Float> percentileSet = new HashSet<Float>();
    percentileSet.add(99.9f);
    Map<Float, Integer> tuplePercentileMap = new HashMap<Float, Integer>();
    tuplePercentileMap.put(99.9f, 2);
    Map<NodeKey, Node> nodeMap = new HashMap<NodeKey, Node>();
    NodeKey key = TestUtil.getNodeKeyOfTuple(tupleList.get(0));
    serviceManager.createNode(tupleList.get(0), nodeMap, percentileSet,
        tuplePercentileMap);
    assert nodeMap.size() == 1;
    Assert.assertNotNull(nodeMap.get(key));
    Node node = nodeMap.get(key);
    Assert.assertEquals(2500l, node.getAggregateMessagesReceived().longValue());
    Assert.assertEquals(2500l, node.getAggregateMessagesSent().longValue());
    Assert.assertNotNull(node.getReceivedMessagesList());
    Assert.assertNotNull(node.getSentMessagesList());
    Assert.assertEquals(1, node.getReceivedMessagesList().size());
    Assert.assertEquals(1, node.getSentMessagesList().size());
    MessageStats rMessageStat = new MessageStats(tupleList.get(0).getTopic(),
        2500l, null);
    MessageStats sMessageStat = new MessageStats(tupleList.get(0).getTopic(),
        2500l, null);
    Assert.assertEquals(node.getReceivedMessagesList().get(0), rMessageStat);
    Assert.assertEquals(node.getSentMessagesList().get(0), sMessageStat);
    tuplePercentileMap.clear();
    tuplePercentileMap.put(99.9f, 10);
    key = TestUtil.getNodeKeyOfTuple(tupleList.get(1));
    serviceManager.createNode(tupleList.get(1), nodeMap, percentileSet,
        tuplePercentileMap);
    assert nodeMap.size() == 1;
    Assert.assertNotNull(nodeMap.get(key));
    node = nodeMap.get(key);
    Assert.assertEquals(5000l, node.getAggregateMessagesReceived().longValue());
    Assert.assertEquals(5000l, node.getAggregateMessagesSent().longValue());
    Assert.assertNotNull(node.getReceivedMessagesList());
    Assert.assertNotNull(node.getSentMessagesList());
    Assert.assertEquals(2, node.getReceivedMessagesList().size());
    Assert.assertEquals(2, node.getSentMessagesList().size());
    rMessageStat = new MessageStats(tupleList.get(0).getTopic(),
        2500l, null);
    sMessageStat = new MessageStats(tupleList.get(0).getTopic(),
        2500l, null);
    Assert.assertTrue(node.getReceivedMessagesList().contains(rMessageStat));
    Assert.assertTrue(node.getSentMessagesList().contains(sMessageStat));
    rMessageStat = new MessageStats(tupleList.get(0).getTopic(),
        2500l, null);
    sMessageStat = new MessageStats(tupleList.get(0).getTopic(),
        2500l, null);
    Assert.assertTrue(node.getReceivedMessagesList().contains(rMessageStat));
    Assert.assertTrue(node.getSentMessagesList().contains(sMessageStat));
  }

  @Test
  public void testHdfsNodeMessageStatsHostName() {
    serviceManager = DataServiceManagerTest.get("" +
        "./src/test/resources/xmlfolder2",
        "./src/test/resources/visualization2.properties", null);
    Tuple hdfsTuple = TestUtil.getHdfsTestTuple();
    Set<Float> percentileSet = new HashSet<Float>();
    percentileSet.add(99.9f);
    Map<Float, Integer> tuplePercentileMap = new HashMap<Float, Integer>();
    tuplePercentileMap.put(99.9f, 2);
    Map<NodeKey, Node> nodeMap = new HashMap<NodeKey, Node>();
    NodeKey key = TestUtil.getNodeKeyOfHdfsOrVipTuple(hdfsTuple);
    serviceManager.createNode(hdfsTuple, nodeMap, percentileSet,
        tuplePercentileMap);
    assert nodeMap.size() == 1;
    Assert.assertNotNull(nodeMap.get(key));
    Node node = nodeMap.get(key);
    Assert.assertEquals(1, node.getReceivedMessagesList().size());
    Assert.assertNotNull(node.getReceivedMessagesList().get(0).getHostname());
    Assert.assertEquals(hdfsTuple.getHostname(), node.getReceivedMessagesList
        ().get(0).getHostname());
    Assert.assertEquals(1, node.getSentMessagesList().size());
    Assert.assertNotNull(node.getSentMessagesList().get(0).getHostname());
    Assert.assertEquals(hdfsTuple.getHostname(), node.getSentMessagesList()
        .get(0).getHostname());
  }

  @Test
  public void testAggregatePercentileMapOfNode() {
    serviceManager = DataServiceManagerTest.get("" +
        "./src/test/resources/xmlfolder2",
        "./src/test/resources/visualization2.properties", null);
    Set<Float> percentileSet = new HashSet<Float>();
    percentileSet.add(70f);
    List<Tuple> tupleList = TestUtil.getTuplesWithSameNodeKeyDiffTopic();
    Map<Float, Integer> tuplePercentileMap1 = new HashMap<Float, Integer>(1);
    tuplePercentileMap1.put(70f, 1);
    Map<Float, Integer> tuplePercentileMap2 = new HashMap<Float, Integer>(1);
    tuplePercentileMap2.put(70f, 10);
    Map<NodeKey, Node> nodeMap = new HashMap<NodeKey, Node>();
    serviceManager.createNode(tupleList.get(0), nodeMap, percentileSet,
        tuplePercentileMap1);
    serviceManager.createNode(tupleList.get(1), nodeMap, percentileSet,
        tuplePercentileMap2);
    Assert.assertEquals(1, nodeMap.size());
    NodeKey key = TestUtil.getNodeKeyOfTuple(tupleList.get(0));
    Assert.assertNotNull(nodeMap.get(key));
    Node node = nodeMap.get(key);
    node.buildPercentileMap();
    Assert.assertEquals(5, node.getPercentileMap().get(70f).intValue());
    tupleList = TestUtil.getCollectorTuples();
    Map<Float, Integer> tuplePercentileMap3 = new HashMap<Float, Integer>(1);
    tuplePercentileMap3.put(70f, 1);
    nodeMap.clear();
    serviceManager.createNode(tupleList.get(0), nodeMap, percentileSet,
        tuplePercentileMap1);
    serviceManager.createNode(tupleList.get(1), nodeMap, percentileSet,
        tuplePercentileMap3);
    serviceManager.createNode(tupleList.get(2), nodeMap, percentileSet,
        tuplePercentileMap2);
    serviceManager.addVIPNodesToNodesList(nodeMap, percentileSet);
    Assert.assertEquals(3, nodeMap.size());
    NodeKey vipKey = TestUtil.getNodeKeyOfHdfsOrVipTuple(TestUtil.getVipTuple
        ());
    Assert.assertNotNull(nodeMap.get(vipKey));
    Node vipNode = nodeMap.get(vipKey);
    Assert.assertEquals(2, vipNode.getPerTopicPercentileMap().size());
    Assert.assertEquals(1, vipNode.getPerTopicPercentileMap().get(tupleList
        .get(0).getTopic()).get(70f).intValue());
    Assert.assertEquals(5, vipNode.getPerTopicPercentileMap().get(tupleList
        .get(1).getTopic()).get(70f).intValue());
    Assert.assertEquals(5, vipNode.getPercentileMap().get(70f).intValue());
  }

  @Test
  public void testFilterParametersSet() {
    serviceManager = DataServiceManagerTest.get("" +
        "./src/test/resources/xmlfolder2",
        "./src/test/resources/visualization2.properties", null);
    Calendar calendar = Calendar.getInstance();
    Date startDate = calendar.getTime();
    calendar.add(Calendar.MINUTE, 5);
    Date endDate = calendar.getTime();
    String start, end, stream, cluster;
    start = auditDateFormatter.format(startDate);
    end = auditDateFormatter.format(endDate);
    stream = "stream1";
    cluster = "cluster1";
    String filterValues = TestUtil.getFilterStringFromParameters(start, end,
        stream, cluster);
    Map<String, String> filterMap = serviceManager.getFilterMap(filterValues);
    Assert.assertEquals(start, filterMap.get(ServerConstants.START_TIME_FILTER));
    Assert.assertEquals(end, filterMap.get(ServerConstants.END_TIME_FILTER));
    Assert.assertEquals(stream, filterMap.get(ServerConstants.STREAM_FILTER));
    Assert.assertEquals(cluster, filterMap.get(ServerConstants.CLUSTER_FILTER));
    stream = "stream2";
    filterValues = TestUtil.getFilterStringFromParameters(start, end,
        stream, cluster);
    filterMap = serviceManager.getFilterMap(filterValues);
    Assert.assertEquals(stream, filterMap.get(ServerConstants.STREAM_FILTER));
    cluster = "cluster1";
    filterValues = TestUtil.getFilterStringFromParameters(start, end,
        stream, cluster);
    filterMap = serviceManager.getFilterMap(filterValues);
    Assert.assertEquals(cluster, filterMap.get(ServerConstants.CLUSTER_FILTER));
  }

  @Test
  public void testCheckIfNodesPassFilters() {
    Connection connection = null;
    try {
      connection = TestUtil.setupDB();
      serviceManager = DataServiceManagerTest.get("" +
          "./src/test/resources/xmlfolder2",
          "./src/test/resources/visualization2.properties",
          "./src/test/resources/audit-feeder.properties");
      auditDateFormatter.setTimeZone(TimeZone.getTimeZone("GMT"));
      String startDate = auditDateFormatter.format(TestUtil.incrementDate
          (TestUtil.getCurrentDate(), -1));
      String endDate = auditDateFormatter.format(TestUtil.incrementDate
          (TestUtil.getCurrentDate(), 5));
      String stream = "testTopic1";
      String cluster = "All";
      String filterValues = TestUtil.getFilterStringFromParameters(startDate,
          endDate, stream, cluster);
      String response = serviceManager.getData(filterValues);
      List<Node> nodeList = TestUtil.getNodeListFromResponse(response);
      Assert.assertEquals(6, nodeList.size());
      stream = "testTopic2";
      filterValues = TestUtil.getFilterStringFromParameters(startDate,
          endDate, stream, cluster);
      response = serviceManager.getData(filterValues);
      nodeList = TestUtil.getNodeListFromResponse(response);
      Assert.assertEquals(5, nodeList.size());
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      TestUtil.shutDownDB(connection);
    }
  }

  @Test
  public void testGetNodes() {
    Connection connection = null;
    try {
      connection = TestUtil.setupDB();
      serviceManager = DataServiceManagerTest.get("" +
          "./src/test/resources/xmlfolder2",
          "./src/test/resources/visualization2.properties",
          "./src/test/resources/audit-feeder.properties");
      auditDateFormatter.setTimeZone(TimeZone.getTimeZone("GMT"));
      String startDate = auditDateFormatter.format(TestUtil.incrementDate
          (TestUtil.getCurrentDate(), -1));
      String endDate = auditDateFormatter.format(TestUtil.incrementDate
          (TestUtil.getCurrentDate(), 5));
      String stream = "All";
      String cluster = "All";
      String filterValues = TestUtil.getFilterStringFromParameters(startDate,
          endDate, stream, cluster);
      String response = serviceManager.getData(filterValues);
      List<Node> nodeList = TestUtil.getNodeListFromResponse(response);
      Assert.assertEquals(8, nodeList.size());
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      TestUtil.shutDownDB(connection);
    }
  }
}
