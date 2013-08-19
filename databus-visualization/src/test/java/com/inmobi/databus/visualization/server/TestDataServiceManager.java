package com.inmobi.databus.visualization.server;

import com.inmobi.databus.visualization.server.util.TestUtil;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class TestDataServiceManager {
  DataServiceManagerTest serviceManager;

  @Test
  public void testAuditStreamNotAdded() {
    serviceManager = DataServiceManagerTest.get("" +
        "./src/test/resources/xmlfolder2", true);
    String response = serviceManager.getStreamAndClusterList();
    try {
      List<String> streamList = TestUtil.getStreamListFromResponse(response);
      assert !streamList.contains(DataServiceManagerTest.AUDIT_STREAM);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testMultipleXmls() {
    serviceManager = DataServiceManagerTest.get("" +
        "./src/test/resources/xmlfolder1", true);
    assert serviceManager.getDataBusConfig().size() == 2;
    String response = serviceManager.getStreamAndClusterList();
    try {
      List<String> streamList = TestUtil.getStreamListFromResponse(response);
      List<String> clusterList = TestUtil.getClusterListFromResponse(response);
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
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testVipNodesStreamsAddedCorrect() {

  }

  @Test
  public void testNodesCreated() {

  }
}
