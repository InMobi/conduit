package com.inmobi.databus.distcp;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

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
import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.DatabusConfigParser;
import com.inmobi.databus.DatabusConstants;
import com.inmobi.databus.DestinationStream;
import com.inmobi.databus.FSCheckpointProvider;
import com.inmobi.databus.SourceStream;
import com.inmobi.databus.TestMiniClusterUtil;
import com.inmobi.databus.local.TestLocalStreamService;

public class MergeMirrorStreamTest extends TestMiniClusterUtil {

  private static final Log LOG = LogFactory.getLog(MergeMirrorStreamTest.class);

  @BeforeMethod
  public void beforeTest() throws Exception{
    Properties prop = new Properties();
    prop.setProperty("com.inmobi.databus.metrics.enabled", "true");
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
    testMergeMirrorStream("test-mss-databus.xml", null, null);
    Assert.assertEquals(ConduitMetrics.getCounter("LocalStreamService","retry.mkDir","test1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("LocalStreamService","retry.rename","test1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("LocalStreamService","commitPaths.count","test1").getCount() , 18);
    Assert.assertEquals(ConduitMetrics.getCounter("LocalStreamService","retry.checkPoint","test1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("MirrorStreamService","retry.mkDir","test1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("MirrorStreamService","retry.exist","test1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("MirrorStreamService","retry.rename","test1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("MirrorStreamService","retry.checkPoint","test1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("MirrorStreamService","commitPaths.count","test1").getCount() , 138);
    Assert.assertEquals(ConduitMetrics.getCounter("MergedStreamService","retry.checkPoint","test1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("MergedStreamService","retry.mkDir","test1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("MergedStreamService","commitPaths.count","test1").getCount() , 18);
    Assert.assertEquals(ConduitMetrics.getCounter("MergedStreamService","retry.exist","test1").getCount() , 0);
  }

  @Test
  public void testMergeMirrorStreamWithMultipleStreams() throws Exception {
    testMergeMirrorStream("test-mss-databus1.xml", null, null);
    Assert.assertEquals(ConduitMetrics.getCounter("LocalStreamService","retry.rename","stream1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("MergedStreamService","retry.mkDir","stream1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("LocalStreamService","retry.rename","stream2").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("LocalStreamService","retry.mkDir","stream2").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("LocalStreamService","retry.mkDir","stream1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("MirrorStreamService","retry.exist","stream2").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("MergedStreamService","retry.mkDir","stream2").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("MergedStreamService","retry.rename","stream2").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("MergedStreamService","retry.rename","stream1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("MirrorStreamService","commitPaths.count","stream2").getCount() , 130);
    Assert.assertEquals(ConduitMetrics.getCounter("MirrorStreamService","retry.checkPoint","stream2").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("MirrorStreamService","retry.rename","stream2").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("MergedStreamService","retry.checkPoint","stream2").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("MergedStreamService","retry.checkPoint","stream1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("MergedStreamService","retry.exist","stream2").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("MirrorStreamService","retry.mkDir","stream2").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("LocalStreamService","retry.checkPoint","stream1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("LocalStreamService","retry.checkPoint","stream2").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("LocalStreamService","commitPaths.count","stream1").getCount() , 18);
    Assert.assertEquals(ConduitMetrics.getCounter("MergedStreamService","retry.exist","stream1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("MergedStreamService","commitPaths.count","stream2").getCount() , 9);
    Assert.assertEquals(ConduitMetrics.getCounter("MergedStreamService","commitPaths.count","stream1").getCount() , 18);
    Assert.assertEquals(ConduitMetrics.getCounter("LocalStreamService","commitPaths.count","stream2").getCount() , 18);
  }

  @Test
  public void testMergeMirrorStreamWithMirror() throws Exception {
    // Test with 2 mirror sites
    testMergeMirrorStream("test-mss-databus_mirror.xml", null, null);
    Assert.assertEquals(ConduitMetrics.getCounter("LocalStreamService","retry.mkDir","test1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("MirrorStreamService","retry.mkDir","test1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("MergedStreamService","retry.exist","test1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("LocalStreamService","retry.rename","test1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("MirrorStreamService","retry.exist","test1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("MergedStreamService","retry.checkPoint","test1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("LocalStreamService","commitPaths.count","test1").getCount() , 18);
    Assert.assertEquals(ConduitMetrics.getCounter("MergedStreamService","retry.mkDir","test1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("MirrorStreamService","retry.rename","test1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("MergedStreamService","commitPaths.count","test1").getCount() , 18);
    Assert.assertEquals(ConduitMetrics.getCounter("MirrorStreamService","retry.checkPoint","test1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("LocalStreamService","retry.checkPoint","test1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("MergedStreamService","retry.rename","test1").getCount() , 0);
  }

  @Test
  public void testMergeStreamWithCurrentClusterName() throws Exception {
    //test where LocalStreamService runs of cluster1, cluster2,
    // cluster3 all run on cluster5
    String clusterName = "testcluster5";
    Set<String> clustersToProcess = new HashSet<String>();
    clustersToProcess.add("testcluster4");
    testMergeMirrorStream("testDatabusWithClusterName.xml", clusterName,
        clustersToProcess);
    Assert.assertEquals(ConduitMetrics.getCounter("LocalStreamService","retry.mkDir","test1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("MergedStreamService","retry.exist","test1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("LocalStreamService","retry.rename","test1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("MergedStreamService","retry.checkPoint","test1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("LocalStreamService","commitPaths.count","test1").getCount() , 36);
    Assert.assertEquals(ConduitMetrics.getCounter("MergedStreamService","retry.mkDir","test1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("MergedStreamService","commitPaths.count","test1").getCount() , 27);
    Assert.assertEquals(ConduitMetrics.getCounter("LocalStreamService","retry.checkPoint","test1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("MergedStreamService","retry.rename","test1").getCount() , 0);
  }

  @Test
  public void testMergeStreamClusterNameParallelClusters() throws Exception {
    //test where LocalStreamService of cluster1 runs on clusters1 and so on
    // but mergedStreamService runs on cluster5
    Set<String> clustersToProcess = new HashSet<String>();
    String currentClusterName = null;

    // start LocalStreamService on cluster2
    clustersToProcess.clear();
    clustersToProcess.add("testcluster2");
    testMergeMirrorStream("testDatabusWithClusterNameParallel.xml",
        currentClusterName, clustersToProcess, false);

    //start LocalStreamService on cluster3
    clustersToProcess.clear();
    clustersToProcess.add("testcluster3");
    testMergeMirrorStream("testDatabusWithClusterNameParallel.xml",
        currentClusterName, clustersToProcess, false);

    clustersToProcess.clear();
    // start LocalStreamService on cluster1 and currentClusterName is set to
    // null as both source and current cluster are same
    clustersToProcess.add("testcluster1");
    testMergeMirrorStream("testDatabusWithClusterNameParallel.xml",
        currentClusterName, clustersToProcess, false);

    //start MergedStreamService of cluster4 on cluster5
    currentClusterName = "testcluster5";
    clustersToProcess.clear();
    clustersToProcess.add("testcluster4");
    testMergeMirrorStream("testDatabusWithClusterNameParallel.xml",
        currentClusterName, clustersToProcess, false);
    Assert.assertEquals(ConduitMetrics.getCounter("LocalStreamService","retry.mkDir","test1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("MergedStreamService","retry.exist","test1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("LocalStreamService","retry.rename","test1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("MergedStreamService","retry.checkPoint","test1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("LocalStreamService","commitPaths.count","test1").getCount() , 36);
    Assert.assertEquals(ConduitMetrics.getCounter("MergedStreamService","retry.mkDir","test1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("MergedStreamService","commitPaths.count","test1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("LocalStreamService","retry.checkPoint","test1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("MergedStreamService","retry.rename","test1").getCount() , 0);
  }

  @Test(groups = { "integration" })
  public void testAllComboMergeMirrorStream() throws Exception {
    // Test with 1 merged stream only
    testMergeMirrorStream("test-mergedss-databus.xml", null, null);

    Assert.assertEquals(ConduitMetrics.getCounter("MergedStreamService","retry.exist","test1").getCount() , 0 );
    Assert.assertEquals(ConduitMetrics.getCounter("MergedStreamService","retry.checkPoint","test1").getCount() , 0 );
    Assert.assertEquals(ConduitMetrics.getCounter("MergedStreamService","retry.mkDir","test1").getCount() , 0 );
    Assert.assertEquals(ConduitMetrics.getCounter("MergedStreamService","commitPaths.count","test1").getCount() , 18 );
    Assert.assertEquals(ConduitMetrics.getCounter("MergedStreamService","retry.rename","test1").getCount() , 0 );
    Assert.assertEquals(ConduitMetrics.getCounter("LocalStreamService","commitPaths.count","test1").getCount(), 18);
    Assert.assertEquals(ConduitMetrics.getCounter("LocalStreamService","retry.checkPoint","test1").getCount(), 0);
    Assert.assertEquals(ConduitMetrics.getCounter("LocalStreamService","retry.rename","test1").getCount(), 0);
    Assert.assertEquals(ConduitMetrics.getCounter("LocalStreamService","retry.mkDir","test1").getCount(), 0);
  }

  @Test(groups = { "integration" })
  public void testAllServices() throws Exception {
    // Test with 1 source and 1 merged stream only
    testMergeMirrorStream("test-mergedss-databus_2.xml", null, null);
    Assert.assertEquals(ConduitMetrics.getCounter("LocalStreamService","retry.mkDir","test1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("LocalStreamService","retry.checkPoint","test1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("LocalStreamService","retry.rename","test1").getCount() , 0);
    Assert.assertEquals(ConduitMetrics.getCounter("LocalStreamService","commitPaths.count","test1").getCount() , 9);
  }

  @BeforeSuite
  public void setup() throws Exception {
    // clean up the test data if any thing is left in the previous runs
    cleanup();
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

  private void testMergeMirrorStream(String filename, String currentClusterName,
      Set<String> additionalClustersToProcess,
      boolean addAllSourceClusters)
          throws Exception {

    DatabusConfigParser parser = new DatabusConfigParser(filename);
    DatabusConfig config = parser.getConfig();
    Set<String> streamsToProcessLocal = new HashSet<String>();
    streamsToProcessLocal.addAll(config.getSourceStreams().keySet());
    System.setProperty(DatabusConstants.DIR_PER_DISTCP_PER_STREAM, "200");

    Cluster currentCluster = null;
    if (currentClusterName != null) {
      currentCluster = config.getClusters().get(currentClusterName);
      Assert.assertNotNull(currentCluster);
      Assert.assertEquals(currentClusterName, currentCluster.getName());
    }

    Set<String> clustersToProcess = new HashSet<String>();
    if(additionalClustersToProcess != null)
      clustersToProcess.addAll(additionalClustersToProcess);
    Set<TestLocalStreamService> localStreamServices =
        new HashSet<TestLocalStreamService>();

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

    LOG.info("Running LocalStream Service");

    for (TestLocalStreamService service : localStreamServices) {
      Thread.currentThread().setName(service.getName());
      service.runPreExecute();
      service.runExecute();
      service.runPostExecute();
    }

    Set<TestMergedStreamService> mergedStreamServices = new HashSet<TestMergedStreamService>();
    Set<TestMirrorStreamService> mirrorStreamServices = new HashSet<TestMirrorStreamService>();

    for (String clusterString : clustersToProcess) {
      Cluster cluster = config.getClusters().get(clusterString);
      cluster.getHadoopConf().set("mapred.job.tracker", "local");

      Set<String> mergedStreamRemoteClusters = new HashSet<String>();
      Set<String> mirroredRemoteClusters = new HashSet<String>();
      Map<String, Set<String>> mergedSrcClusterToStreamsMap = new HashMap<String, Set<String>>();
      Map<String, Set<String>> mirrorSrcClusterToStreamsMap = new HashMap<String, Set<String>>();
      for (DestinationStream cStream : cluster.getDestinationStreams().values()) {
        //Start MergedStreamConsumerService instances for this cluster for each cluster
        //from where it has to fetch a partial stream and is hosting a primary stream
        //Start MirroredStreamConsumerService instances for this cluster for each cluster
        //from where it has to mirror mergedStreams

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
        TestMergedStreamService remoteMergeService =
            new TestMergedStreamService(config,
                config.getClusters().get(remote), cluster,
                currentCluster,
                mergedSrcClusterToStreamsMap.get(remote));
        mergedStreamServices.add(remoteMergeService);
        if (currentCluster != null)
          Assert.assertEquals(remoteMergeService.getCurrentCluster(),
              currentCluster);
        else
          Assert.assertEquals(remoteMergeService.getCurrentCluster(),
              cluster);
      }
      for (String remote : mirroredRemoteClusters) {
        TestMirrorStreamService remoteMirrorService =
            new TestMirrorStreamService(config,
                config.getClusters().get(remote), cluster,
                currentCluster,
                mirrorSrcClusterToStreamsMap.get(remote));
        mirrorStreamServices.add(remoteMirrorService);
        if (currentCluster != null)
          Assert.assertEquals(remoteMirrorService.getCurrentCluster(),
              currentCluster);
        else
          Assert.assertEquals(remoteMirrorService.getCurrentCluster(),
              cluster);
      }
    }

    LOG.info("Running MergedStream Service");

    for (TestMergedStreamService service : mergedStreamServices) {
      Thread.currentThread().setName(service.getName());
      service.testRequalification();
      service.runPreExecute();
      service.runExecute();
      service.runPostExecute();

    }

    LOG.info("Running MirrorStreamService Service");

    for (TestMirrorStreamService service : mirrorStreamServices) {
      Thread.currentThread().setName(service.getName());
      service.testRequalification();
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
}
