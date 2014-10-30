package com.inmobi.conduit.distcp;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.inmobi.conduit.AbstractService;
import com.inmobi.conduit.Cluster;
import com.inmobi.conduit.ConduitConfigParser;
import com.inmobi.conduit.ConduitConstants;
import com.inmobi.conduit.DestinationStream;
import com.inmobi.conduit.FSCheckpointProvider;
import com.inmobi.conduit.SourceStream;
import com.inmobi.conduit.TestMiniClusterUtil;
import com.inmobi.conduit.metrics.AbsoluteGauge;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import com.inmobi.conduit.metrics.ConduitMetrics;
import com.inmobi.conduit.metrics.SlidingTimeWindowGauge;
import com.inmobi.conduit.ConduitConfig;
import com.inmobi.conduit.local.TestLocalStreamService;
import com.inmobi.messaging.publisher.MessagePublisher;
import com.inmobi.messaging.publisher.MessagePublisherFactory;

public class MergeMirrorStreamTest extends TestMiniClusterUtil {

  private static final Log LOG = LogFactory.getLog(MergeMirrorStreamTest.class);

  @BeforeMethod
  public void beforeTest() throws Exception{
    Properties prop = new Properties();
    prop.setProperty("com.inmobi.conduit.metrics.enabled", "true");
    prop.setProperty("com.inmobi.conduit.metrics.slidingwindowtime", "100000000");
    ConduitMetrics.init(prop);
    ConduitMetrics.startAll();
  }

  @AfterMethod
  public void afterTest() throws Exception{
    ConduitMetrics.stopAll();;
  }

  /*
   * Here is the basic idea, create two clusters of different rootdir paths run
   * the local stream service to create all the files in streams_local directory
   * run the merge stream service and verify all the paths are visible in
   * primary cluster
   */
  /**
   * @throws Exception
   */
  @Test
  public void testMergeMirrorStream() throws Exception {
    testMergeMirrorStream("test-mss-conduit.xml", null, null);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService", AbstractService.FAILURES,"test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService", AbstractService.RUNTIME,"test1").getValue().longValue() , 0);
    Assert.assertTrue(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService", AbstractService.COMMIT_TIME, "test1").getValue().longValue() < 60000);
    Assert.assertTrue(ConduitMetrics.<AbsoluteGauge>getMetric("LocalStreamService", AbstractService.LAST_FILE_PROCESSED, "test1").getValue().longValue() > 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService", AbstractService.RETRY_MKDIR,"test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService", AbstractService.RETRY_RENAME, "test1").getValue().longValue(), 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService", AbstractService.FILES_COPIED_COUNT, "test1").getValue().longValue(), 18);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService", AbstractService.RETRY_CHECKPOINT, "test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MirrorStreamService", AbstractService.FAILURES,"test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MirrorStreamService", AbstractService.RUNTIME,"test1").getValue().longValue() , 0);
    Assert.assertTrue(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MirrorStreamService", AbstractService.COMMIT_TIME, "test1").getValue().longValue() < 60000);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MirrorStreamService",AbstractService.RETRY_MKDIR, "test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MirrorStreamService", AbstractService.RETRY_RENAME, "test1").getValue().longValue(), 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MirrorStreamService", AbstractService.RETRY_CHECKPOINT, "test1").getValue().longValue(), 0);
    Assert.assertTrue(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MirrorStreamService", AbstractService.EMPTYDIR_CREATE, "test1").getValue().longValue() >= 120);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MirrorStreamService",AbstractService.FILES_COPIED_COUNT,"test1").getValue().longValue() , 18);
    Assert.assertTrue(ConduitMetrics.<AbsoluteGauge>getMetric("MirrorStreamService", AbstractService.LAST_FILE_PROCESSED, "test1").getValue().longValue() > 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService", AbstractService.FAILURES, "test1").getValue().longValue(), 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService", AbstractService.RUNTIME, "test1").getValue().longValue(), 0);
    Assert.assertTrue(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService", AbstractService.COMMIT_TIME, "test1").getValue().longValue() < 60000);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService",AbstractService.RETRY_CHECKPOINT,"test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService",AbstractService.RETRY_MKDIR,"test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService",AbstractService.FILES_COPIED_COUNT,"test1").getValue().longValue() , 18);
    Assert.assertTrue(ConduitMetrics.<AbsoluteGauge>getMetric("MergedStreamService", AbstractService.LAST_FILE_PROCESSED, "test1").getValue().longValue() > 0);
    Assert.assertTrue(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService", AbstractService.JOB_EXECUTION_TIME, "test1").getValue().longValue() >= 0);
  }

  @Test
  public void testAuditForWorker() throws Exception {
    Set<TestLocalStreamService> localStreamServices = new HashSet<TestLocalStreamService>();
    Set<TestMergedStreamService> mergedStreamServices = new HashSet<TestMergedStreamService>();
    Set<TestMirrorStreamService> mirrorStreamServices = new HashSet<TestMirrorStreamService>();

    initializeConduit("test-mss-conduit.xml", null, null, true,
        localStreamServices, mergedStreamServices, mirrorStreamServices);
    Path localStreamlFile = new Path("file:/tmp/mergeservicetest/testcluster1/mergeservice/" +
        "streams_local/test1/2013/10/07/16/56/testcluster2-test1-2013-10-07-16-55_00002.gz");
    Path mergeStreamFile = new Path("file:/tmp/mergeservicetest/testcluster1/mergeservice/" +
    		"streams/test1/2013/10/07/16/56/testcluster2-test1-2013-10-07-16-55_00002.gz");
    for (TestLocalStreamService service : localStreamServices) {
      Assert.assertEquals("test1", service.getTopicNameFromDestnPath(localStreamlFile));
      break;
    }

    for (TestMergedStreamService service : mergedStreamServices) {
      Assert.assertEquals("test1", service.getTopicNameFromDestnPath(mergeStreamFile));
      break;
    }
  }

  @Test
  public void testMergeMirrorStreamWithMultipleStreams() throws Exception {
    testMergeMirrorStream("test-mss-conduit1.xml", null, null);

    Assert.assertTrue(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService", AbstractService.COMMIT_TIME, "stream1").getValue().longValue() < 60000);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService", AbstractService.RUNTIME, "stream1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService", AbstractService.FAILURES, "stream1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService", AbstractService.RETRY_RENAME, "stream1").getValue().longValue(), 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService", AbstractService.RETRY_MKDIR, "stream1").getValue().longValue(), 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService",AbstractService.RETRY_CHECKPOINT,"stream1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService",AbstractService.FILES_COPIED_COUNT, "stream1").getValue().longValue() , 18);
    Assert.assertTrue(ConduitMetrics.<AbsoluteGauge>getMetric("LocalStreamService", AbstractService.LAST_FILE_PROCESSED, "stream1").getValue().longValue() > 0);
    Assert.assertTrue(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService",AbstractService.COMMIT_TIME,"stream1").getValue().longValue() < 60000);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService",AbstractService.RUNTIME,"stream1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService",AbstractService.FAILURES,"stream1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService",AbstractService.FILES_COPIED_COUNT,"stream1").getValue().longValue() , 18);
    Assert.assertTrue(ConduitMetrics.<AbsoluteGauge>getMetric("MergedStreamService", AbstractService.LAST_FILE_PROCESSED, "stream1").getValue().longValue() > 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService",AbstractService.RETRY_MKDIR, "stream1").getValue().longValue(), 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService",AbstractService.RETRY_RENAME,"stream1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService",AbstractService.RETRY_CHECKPOINT,"stream1").getValue().longValue() , 0);

    Assert.assertTrue(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService", AbstractService.COMMIT_TIME,"stream2").getValue().longValue() < 60000);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService",AbstractService.RUNTIME,"stream2").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService",AbstractService.FAILURES,"stream2").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService",AbstractService.RETRY_RENAME,"stream2").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService",AbstractService.RETRY_MKDIR, "stream2").getValue().longValue(), 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService",AbstractService.FILES_COPIED_COUNT, "stream2").getValue().longValue(), 18);
    Assert.assertTrue(ConduitMetrics.<AbsoluteGauge>getMetric("LocalStreamService", AbstractService.LAST_FILE_PROCESSED, "stream2").getValue().longValue() > 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService",AbstractService.RETRY_CHECKPOINT, "stream2").getValue().longValue(),0);
    Assert.assertTrue(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService", AbstractService.COMMIT_TIME,"stream2").getValue().longValue() < 60000);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService",AbstractService.RUNTIME,"stream2").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService", AbstractService.FAILURES,"stream2").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService",AbstractService.RETRY_MKDIR, "stream2").getValue().longValue(), 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService",AbstractService.RETRY_RENAME, "stream2").getValue().longValue(), 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService",AbstractService.RETRY_CHECKPOINT,"stream2").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService", AbstractService.FILES_COPIED_COUNT, "stream2").getValue().longValue(), 9);
    Assert.assertTrue(ConduitMetrics.<AbsoluteGauge>getMetric("MergedStreamService", AbstractService.LAST_FILE_PROCESSED, "stream2").getValue().longValue() > 0);
    Assert.assertTrue(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MirrorStreamService", AbstractService.COMMIT_TIME, "stream2").getValue().longValue() < 60000);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MirrorStreamService",AbstractService.RUNTIME,"stream2").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MirrorStreamService",AbstractService.FAILURES,"stream2").getValue().longValue() , 0);
    Assert.assertTrue(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MirrorStreamService",AbstractService.EMPTYDIR_CREATE, "stream2").getValue().longValue() >= 120);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MirrorStreamService",AbstractService.FILES_COPIED_COUNT,"stream2").getValue().longValue() , 9);
    Assert.assertTrue(ConduitMetrics.<AbsoluteGauge>getMetric("MirrorStreamService", AbstractService.LAST_FILE_PROCESSED, "stream2").getValue().longValue() > 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MirrorStreamService",AbstractService.RETRY_CHECKPOINT,"stream2").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MirrorStreamService",AbstractService.RETRY_RENAME,"stream2").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MirrorStreamService",AbstractService.RETRY_MKDIR,"stream2").getValue().longValue() , 0);
  }

  @Test
  public void testMergeMirrorStreamWithMirror() throws Exception {
    // Test with 2 mirror sites
    testMergeMirrorStream("test-mss-conduit_mirror.xml", null, null);
    Assert.assertTrue(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService", AbstractService.COMMIT_TIME, "test1").getValue().longValue() < 60000);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService", AbstractService.RUNTIME, "test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService", AbstractService.FAILURES, "test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService",AbstractService.RETRY_MKDIR,"test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService",AbstractService.RETRY_RENAME,"test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService",AbstractService.FILES_COPIED_COUNT,"test1").getValue().longValue() , 18);
    Assert.assertTrue(ConduitMetrics.<AbsoluteGauge>getMetric("LocalStreamService", AbstractService.LAST_FILE_PROCESSED, "test1").getValue().longValue() > 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService",AbstractService.RETRY_CHECKPOINT,"test1").getValue().longValue() , 0);
    Assert.assertTrue(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService", AbstractService.COMMIT_TIME, "test1").getValue().longValue() < 60000);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService", AbstractService.RUNTIME, "test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService", AbstractService.FAILURES, "test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService",AbstractService.RETRY_RENAME,"test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService",AbstractService.RETRY_CHECKPOINT,"test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService",AbstractService.RETRY_MKDIR,"test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService",AbstractService.FILES_COPIED_COUNT,"test1").getValue().longValue() , 18);
    Assert.assertTrue(ConduitMetrics.<AbsoluteGauge>getMetric("MergedStreamService", AbstractService.LAST_FILE_PROCESSED, "test1").getValue().longValue() > 0);
    Assert.assertTrue(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MirrorStreamService", AbstractService.COMMIT_TIME, "test1").getValue().longValue() < 60000);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MirrorStreamService", AbstractService.RUNTIME,"test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MirrorStreamService", AbstractService.FAILURES,"test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MirrorStreamService",AbstractService.RETRY_MKDIR,"test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MirrorStreamService",AbstractService.RETRY_RENAME,"test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MirrorStreamService",AbstractService.RETRY_CHECKPOINT,"test1").getValue().longValue() , 0);
  }

  @Test
  public void testMergeStreamWithCurrentClusterName() throws Exception {
    // test where LocalStreamService runs of cluster1, cluster2,
    // cluster3 all run on cluster5
    String clusterName = "testcluster5";
    Set<String> clustersToProcess = new HashSet<String>();
    clustersToProcess.add("testcluster4");
    testMergeMirrorStream("testConduitWithClusterName.xml", clusterName,
        clustersToProcess);
    Assert.assertTrue(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService", AbstractService.COMMIT_TIME, "test1").getValue().longValue() < 60000);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService", AbstractService.RUNTIME, "test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService", AbstractService.FAILURES, "test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService",AbstractService.RETRY_MKDIR,"test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService",AbstractService.RETRY_RENAME,"test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService",AbstractService.RETRY_CHECKPOINT,"test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService",AbstractService.FILES_COPIED_COUNT,"test1").getValue().longValue() , 36);
    Assert.assertTrue(ConduitMetrics.<AbsoluteGauge>getMetric("LocalStreamService", AbstractService.LAST_FILE_PROCESSED, "test1").getValue().longValue() > 0);
    Assert.assertTrue(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService", AbstractService.COMMIT_TIME, "test1").getValue().longValue() < 60000);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService", AbstractService.RUNTIME, "test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService", AbstractService.FAILURES, "test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService",AbstractService.RETRY_MKDIR,"test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService",AbstractService.RETRY_CHECKPOINT,"test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService",AbstractService.FILES_COPIED_COUNT,"test1").getValue().longValue() , 27);
    Assert.assertTrue(ConduitMetrics.<AbsoluteGauge>getMetric("MergedStreamService", AbstractService.LAST_FILE_PROCESSED, "test1").getValue().longValue() > 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService",AbstractService.RETRY_RENAME,"test1").getValue().longValue() , 0);
  }

  @Test
  public void testMergeStreamClusterNameParallelClusters() throws Exception {
    // test where LocalStreamService of cluster1 runs on clusters1 and so on
    // but mergedStreamService runs on cluster5
    Set<String> clustersToProcess = new HashSet<String>();
    String currentClusterName = null;

    // start LocalStreamService on cluster2
    clustersToProcess.clear();
    clustersToProcess.add("testcluster2");
    testMergeMirrorStream("testConduitWithClusterNameParallel.xml",
        currentClusterName, clustersToProcess, false);

    // start LocalStreamService on cluster3
    clustersToProcess.clear();
    clustersToProcess.add("testcluster3");
    testMergeMirrorStream("testConduitWithClusterNameParallel.xml",
        currentClusterName, clustersToProcess, false);

    clustersToProcess.clear();
    // start LocalStreamService on cluster1 and currentClusterName is set to
    // null as both source and current cluster are same
    clustersToProcess.add("testcluster1");
    testMergeMirrorStream("testConduitWithClusterNameParallel.xml",
        currentClusterName, clustersToProcess, false);

    // start MergedStreamService of cluster4 on cluster5
    currentClusterName = "testcluster5";
    clustersToProcess.clear();
    clustersToProcess.add("testcluster4");
    testMergeMirrorStream("testConduitWithClusterNameParallel.xml",
        currentClusterName, clustersToProcess, false);
    Assert.assertTrue(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService", AbstractService.COMMIT_TIME, "test1").getValue().longValue() < 60000);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService", AbstractService.RUNTIME, "test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService", AbstractService.FAILURES, "test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService",AbstractService.RETRY_MKDIR,"test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService",AbstractService.RETRY_RENAME,"test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService",AbstractService.RETRY_CHECKPOINT,"test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService",AbstractService.FILES_COPIED_COUNT,"test1").getValue().longValue() , 36);
    Assert.assertTrue(ConduitMetrics.<AbsoluteGauge>getMetric("LocalStreamService", AbstractService.LAST_FILE_PROCESSED, "test1").getValue().longValue() > 0);
    Assert.assertTrue(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService", AbstractService.COMMIT_TIME, "test1").getValue().longValue() < 60000);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService", AbstractService.RUNTIME, "test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService", AbstractService.FAILURES, "test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService",AbstractService.RETRY_CHECKPOINT,"test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService",AbstractService.RETRY_MKDIR,"test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService",AbstractService.FILES_COPIED_COUNT,"test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService",AbstractService.RETRY_RENAME,"test1").getValue().longValue() , 0);
  }

  @Test(groups = { "integration" })
  public void testAllComboMergeMirrorStream() throws Exception {
    // Test with 1 merged stream only
    testMergeMirrorStream("test-mergedss-conduit.xml", null, null);

    Assert.assertTrue(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService", AbstractService.COMMIT_TIME, "test1").getValue().longValue() < 60000);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService", AbstractService.RUNTIME, "test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService", AbstractService.FAILURES, "test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService",AbstractService.RETRY_CHECKPOINT,"test1").getValue().longValue() , 0 );
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService",AbstractService.RETRY_MKDIR,"test1").getValue().longValue() , 0 );
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService",AbstractService.FILES_COPIED_COUNT,"test1").getValue().longValue() , 18 );
    Assert.assertTrue(ConduitMetrics.<AbsoluteGauge>getMetric("MergedStreamService", AbstractService.LAST_FILE_PROCESSED, "test1").getValue().longValue() > 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("MergedStreamService",AbstractService.RETRY_RENAME,"test1").getValue().longValue() , 0 );
    Assert.assertTrue(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService", AbstractService.COMMIT_TIME, "test1").getValue().longValue() < 60000);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService", AbstractService.RUNTIME, "test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService", AbstractService.FAILURES, "test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService",AbstractService.FILES_COPIED_COUNT,"test1").getValue().longValue(), 18);
    Assert.assertTrue(ConduitMetrics.<AbsoluteGauge>getMetric("LocalStreamService", AbstractService.LAST_FILE_PROCESSED, "test1").getValue().longValue() > 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService",AbstractService.RETRY_CHECKPOINT,"test1").getValue().longValue(), 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService",AbstractService.RETRY_RENAME,"test1").getValue().longValue(), 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService",AbstractService.RETRY_MKDIR,"test1").getValue().longValue(), 0);
  }

  @Test(groups = { "integration" })
  public void testAllServices() throws Exception {
    // Test with 1 source and 1 merged stream only
    testMergeMirrorStream("test-mergedss-conduit_2.xml", null, null);
    Assert.assertTrue(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService", AbstractService.COMMIT_TIME, "test1").getValue().longValue() < 60000);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService", AbstractService.RUNTIME, "test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService", AbstractService.FAILURES, "test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService",AbstractService.RETRY_MKDIR,"test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService",AbstractService.RETRY_CHECKPOINT,"test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService",AbstractService.RETRY_RENAME,"test1").getValue().longValue() , 0);
    Assert.assertEquals(ConduitMetrics.<SlidingTimeWindowGauge>getMetric("LocalStreamService",AbstractService.FILES_COPIED_COUNT,"test1").getValue().longValue() , 9);
    Assert.assertTrue(ConduitMetrics.<AbsoluteGauge>getMetric("LocalStreamService", AbstractService.LAST_FILE_PROCESSED, "test1").getValue().longValue() > 0);
  }

  @BeforeSuite
  public void setup() throws Exception {
    // clean up the test data if any thing is left in the previous runs
    cleanup();
    System.setProperty(ConduitConstants.AUDIT_ENABLED_KEY, "true");
    super.setup(2, 6, 1);
  }

  @AfterSuite
  public void cleanup() throws Exception {
    super.cleanup();
  }


  private void testMergeMirrorStream(String filename, String currentClusterName,
      Set<String> additionalClustersToProcess)
          throws Exception {
    testMergeMirrorStream(filename, currentClusterName,
        additionalClustersToProcess, true);
  }

  private void testMergeMirrorStream(String filename,
      String currentClusterName, Set<String> additionalClustersToProcess,
      boolean addAllSourceClusters) throws Exception {

    Set<TestLocalStreamService> localStreamServices = new HashSet<TestLocalStreamService>();
    Set<TestMergedStreamService> mergedStreamServices = new HashSet<TestMergedStreamService>();
    Set<TestMirrorStreamService> mirrorStreamServices = new HashSet<TestMirrorStreamService>();

    initializeConduit(filename, currentClusterName, additionalClustersToProcess,
        addAllSourceClusters, localStreamServices, mergedStreamServices,
        mirrorStreamServices);

    LOG.info("Running LocalStream Service");

    for (TestLocalStreamService service : localStreamServices) {
      Thread.currentThread().setName(service.getName());
      service.runPreExecute();
      service.runExecute();
      service.runPostExecute();
    }

    LOG.info("Running MergedStream Service");

    for (TestMergedStreamService service : mergedStreamServices) {
      Thread.currentThread().setName(service.getName());
      service.runPreExecute();
      service.runExecute();
      service.runPostExecute();

    }

    LOG.info("Running MirrorStreamService Service");

    for (TestMirrorStreamService service : mirrorStreamServices) {
      Thread.currentThread().setName(service.getName());
      service.runPreExecute();
      service.runExecute();
      service.runPostExecute();

    }

    LOG.info("Cleaning up leftovers");

    for (TestLocalStreamService service : localStreamServices) {
      service.getFileSystem().delete(
          new Path(service.getCluster().getRootDir()), true);
    }
  }

  private void initializeConduit(String filename, String currentClusterName,
      Set<String> additionalClustersToProcess, boolean addAllSourceClusters,
      Set<TestLocalStreamService> localStreamServices,
      Set<TestMergedStreamService> mergedStreamServices,
      Set<TestMirrorStreamService> mirrorStreamServices)
          throws Exception {
    ConduitConfigParser parser = new ConduitConfigParser(filename);
    ConduitConfig config = parser.getConfig();

    Set<String> streamsToProcessLocal = new HashSet<String>();
    streamsToProcessLocal.addAll(config.getSourceStreams().keySet());
    System.setProperty(ConduitConstants.DIR_PER_DISTCP_PER_STREAM, "200");
    MessagePublisher publisher = MessagePublisherFactory.create();
    Cluster currentCluster = null;
    if (currentClusterName != null) {
      currentCluster = config.getClusters().get(currentClusterName);
      Assert.assertNotNull(currentCluster);
      Assert.assertEquals(currentClusterName, currentCluster.getName());
    }

    Set<String> clustersToProcess = new HashSet<String>();
    if (additionalClustersToProcess != null)
      clustersToProcess.addAll(additionalClustersToProcess);

    if (addAllSourceClusters) {
      for (SourceStream sStream : config.getSourceStreams().values()) {
        for (String cluster : sStream.getSourceClusters()) {
          clustersToProcess.add(cluster);
        }
      }
    }

    for (String clusterName : clustersToProcess) {
      Cluster cluster = config.getClusters().get(clusterName);
      cluster.getHadoopConf().set("mapred.job.tracker",
          super.CreateJobConf().get("mapred.job.tracker"));
      TestLocalStreamService service = new TestLocalStreamService(config,
          cluster, currentCluster,new FSCheckpointProvider(cluster
              .getCheckpointDir()), streamsToProcessLocal);
      localStreamServices.add(service);
      service.getFileSystem().delete(
          new Path(service.getCluster().getRootDir()), true);
    }

    for (String clusterString : clustersToProcess) {
      Cluster cluster = config.getClusters().get(clusterString);
      cluster.getHadoopConf().set("mapred.job.tracker", "local");

      Set<String> mergedStreamRemoteClusters = new HashSet<String>();
      Set<String> mirroredRemoteClusters = new HashSet<String>();
      Map<String, Set<String>> mergedSrcClusterToStreamsMap = new HashMap<String, Set<String>>();
      Map<String, Set<String>> mirrorSrcClusterToStreamsMap = new HashMap<String, Set<String>>();
      for (DestinationStream cStream : cluster.getDestinationStreams().values()) {
        // Start MergedStreamConsumerService instances for this cluster for each
        // cluster
        // from where it has to fetch a partial stream and is hosting a primary
        // stream
        // Start MirroredStreamConsumerService instances for this cluster for
        // each cluster
        // from where it has to mirror mergedStreams


        if (cStream.isPrimary()) {
          for (String cName : config.getSourceStreams().get(cStream.getName())
              .getSourceClusters()) {
            mergedStreamRemoteClusters.add(cName);
            if (mergedSrcClusterToStreamsMap.get(cName) == null) {
              Set<String> tmp = new HashSet<String>();
              tmp.add(cStream.getName());
              mergedSrcClusterToStreamsMap.put(cName, tmp);
            } else {
              mergedSrcClusterToStreamsMap.get(cName).add(cStream.getName());
            }
          }
        }
        if (!cStream.isPrimary()) {
          Cluster primaryCluster = config
              .getPrimaryClusterForDestinationStream(cStream.getName());
          if (primaryCluster != null) {
            mirroredRemoteClusters.add(primaryCluster.getName());
            String clusterName = primaryCluster.getName();
            if (mirrorSrcClusterToStreamsMap.get(clusterName) == null) {
              Set<String> tmp = new HashSet<String>();
              tmp.add(cStream.getName());
              mirrorSrcClusterToStreamsMap.put(clusterName, tmp);
            } else {
              mirrorSrcClusterToStreamsMap.get(clusterName).add(
                  cStream.getName());
            }
          }
        }
      }

      for (String remote : mergedStreamRemoteClusters) {
        TestMergedStreamService remoteMergeService = new TestMergedStreamService(
            config, config.getClusters().get(remote), cluster, currentCluster,
            mergedSrcClusterToStreamsMap.get(remote));
        mergedStreamServices.add(remoteMergeService);
        if (currentCluster != null)
          Assert.assertEquals(remoteMergeService.getCurrentCluster(),
              currentCluster);
        else
          Assert.assertEquals(remoteMergeService.getCurrentCluster(), cluster);
      }
      for (String remote : mirroredRemoteClusters) {
        TestMirrorStreamService remoteMirrorService = new TestMirrorStreamService(
            config, config.getClusters().get(remote), cluster, currentCluster,
            mirrorSrcClusterToStreamsMap.get(remote));
        mirrorStreamServices.add(remoteMirrorService);
        if (currentCluster != null)
          Assert.assertEquals(remoteMirrorService.getCurrentCluster(),
              currentCluster);
        else
          Assert.assertEquals(remoteMirrorService.getCurrentCluster(), cluster);
      }
    }

  }
}
