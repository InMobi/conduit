package com.inmobi.conduit.metrics;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestMetricsUtil {

  /**
   * Sample path to expect
   * file:/tmp/databussimpleservice/testcluster2/localstreamcommit
   * /streams_local/stream1/2013/10/25/12/10
   * 
   */
  @Test
  public void testGetStreamNameFromPath() {
    String streamName = MetricsUtil
        .getStreamNameFromPath("/tmp/databussimpleservice/testcluster2/localstreamcommit/streams_local/stream1/2013/10/25/12/10");
    Assert.assertNotNull(streamName);
    Assert.assertEquals(streamName, "stream1");

  }

  @Test
  public void testGetStreamNameFromTmpPath2() {
    String streamName = MetricsUtil
        .getStreamNameFromTmpPath("file:/tmp/mergeservicetest/testcluster1/mergeservice/system"
            + "/tmp/distcp_mergedStream_testcluster2_testcluster1_test1@/test1/testcluster2-test1-2013-10-25-23-28_00001.gz");
    Assert.assertNotNull(streamName);
    Assert.assertEquals(streamName, "test1");
  }

  @Test
  public void testGetStreamNameFromTmpPath1() {
    String streamName = MetricsUtil
        .getStreamNameFromTmpPath("file:/tmp/mergeservicetest/testcluster2/mergeonlyservice/data/test1/testcluster2/test1-2013-10-25-23-30_00001");
    Assert.assertNotNull(streamName);
    Assert.assertEquals(streamName, "test1");
  }

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
