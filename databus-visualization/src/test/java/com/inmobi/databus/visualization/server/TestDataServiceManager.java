package com.inmobi.databus.visualization.server;

import com.inmobi.databus.visualization.client.util.ClientDataHelper;
import org.junit.Test;

import java.util.List;

public class TestDataServiceManager {
  DataServiceManagerTest serviceManager;

  @Test
  public void testAuditStreamNotAdded() {
    serviceManager = DataServiceManagerTest.get("" +
        "./src/test/resources/xmlfolder2", true);
    String response = serviceManager.getStreamAndClusterList();
    List<String> streamList = ClientDataHelper.getInstance()
        .getStreamsListFromLoadMainPanelResponse(response);
    assert !streamList.contains(DataServiceManagerTest.AUDIT_STREAM);
  }

  @Test
  public void testMultipleXmls() {
    serviceManager = DataServiceManagerTest.get("" +
        "./src/test/resources/xmlfolder1", true);
    assert serviceManager.getDataBusConfig().size() == 2;
    String response = serviceManager.getStreamAndClusterList();
    List<String> streamList = ClientDataHelper.getInstance()
        .getStreamsListFromLoadMainPanelResponse(response);
    List<String> clusterList = ClientDataHelper.getInstance()
        .getClusterListFromLoadMainPanelResponse(response);
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

  }

  @Test
  public void testNewNodeCreatedFromTuple() {

  }

  @Test
  public void testNodeUpdatedWithNewTupleInfo() {
    //test new node aggregate received has incremented correctly

  }

  @Test
  public void testHdfsNodeMessageStatsHostName() {

  }

  @Test
  public void testTierWiseLatencyMap() {

  }

  @Test
  public void testMergeLists() {

  }

  @Test
  public void testNodeKeyOfNode() {

  }

  @Test
  public void testNodesEqual() {

  }

  @Test
  public void testAggregatePercentileMapOfNode() {
    //test buildPercentileMap() method of map
  }

  @Test
  public void testFilterParametersSet() {
    //test parameters got from helper methods are correct
  }

  @Test
  public void testTuplesReturnedPassFilters() {

  }
}
