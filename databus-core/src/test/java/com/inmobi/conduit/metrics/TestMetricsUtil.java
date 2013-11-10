package com.inmobi.conduit.metrics;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestMetricsUtil {

  /**
   * Sample checkpoint to expect TestLocalStreamService_stream1_testcluster2
   */
  @Test
  public void testGetSteamNameFromCheckPointKey() {
    String streamName = MetricsUtil
        .getSteamNameFromCheckPointKey("TestLocalStreamService_stream1_testcluster2");
    Assert.assertNotNull(streamName);
    Assert.assertEquals(streamName, "stream1");

  }

}
