package com.inmobi.conduit.distcp;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.conduit.Cluster;
import com.inmobi.conduit.Conduit;
import com.inmobi.conduit.ConduitConfig;
import com.inmobi.conduit.ConduitConfigParser;
import com.inmobi.conduit.ConduitConstants;
import com.inmobi.conduit.DestinationStream;
import com.inmobi.conduit.FSCheckpointProvider;
import com.inmobi.conduit.SourceStream;
import com.inmobi.conduit.TestMiniClusterUtil;
import com.inmobi.conduit.local.TestLocalStreamPartition;
import com.inmobi.conduit.local.TestLocalStreamService;
import com.inmobi.conduit.utils.CalendarHelper;
import com.inmobi.conduit.utils.TestHCatUtil;

public class MergeMirrorStreamPartitionTest extends TestMiniClusterUtil {
  /*private static final Log LOG = LogFactory.getLog(MergeMirrorStreamPartitionTest.class);

  private List<HCatClientUtil> hcatUtilList = new ArrayList<HCatClientUtil>();
  private static final int msPort1 = 20104;
  private static final int msPort2 = 20105;
  private static final String DB_NAME = "conduit";
  private static final String TABLE_NAME_PREFIX = "conduit";
  private static final String LOCAL_TABLE_NAME_PREFIX = TABLE_NAME_PREFIX + "_local";
  private static Date lastAddedPartTime;

  public static Date getLastAddedPartTime() {
    return lastAddedPartTime;
  }

  @BeforeTest
  public void setup() throws Exception{
    
    super.setup(2, 6, 1);
    System.setProperty(ConduitConstants.AUDIT_ENABLED_KEY, "true");

    Conduit.setHCatEnabled(true);
    
    HiveConf hcatConf1 = TestHCatUtil.getHcatConf(msPort1, "target/metaStore1", "metadb1");
    HiveConf hcatConf2 = TestHCatUtil.getHcatConf(msPort2, "target/metaStore2", "metadb2");

    TestHCatUtil.startMetaStoreServer(hcatConf1, msPort1);
    TestHCatUtil.startMetaStoreServer(hcatConf2, msPort2);
    Thread.sleep(10000);

    HCatClientUtil hcatUtil1 = TestHCatUtil.getHCatUtil(hcatConf1);
    TestHCatUtil.createHCatClients(hcatConf1, hcatUtil1);
    HCatClientUtil hcatUtil2 = TestHCatUtil.getHCatUtil(hcatConf2);
    TestHCatUtil.createHCatClients(hcatConf2, hcatUtil2);
    hcatUtilList.add(hcatUtil1);
    hcatUtilList.add(hcatUtil2);
  }

  @AfterTest
  public void cleanup() throws Exception{
    for (HCatClientUtil hcatUtil : hcatUtilList) {
      hcatUtil.close();
    }
    super.cleanup();
    Conduit.setHCatEnabled(false);
  }

  @Test
  public void testMergeMirrorStreamPartitionTest() throws Exception {
    testMergeMirrorStreamPartitionTest("test-mss-hcat-conduit-1.xml", null, null);
  }

  private void testMergeMirrorStreamPartitionTest(String filename, String currentClusterName,
      Set<String> additionalClustersToProcess)
          throws Exception {
    testMergeMirrorStreamPartitionTest(filename, currentClusterName,
        additionalClustersToProcess, true);
  }

  private void testMergeMirrorStreamPartitionTest(String filename,
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
      service.prepareLastAddedPartitionMap();
      service.runPreExecute();
      service.runExecute();
      service.runPostExecute();
      service.clearPathPartitionTable();
    }

    LOG.info("Running MergedStream Service");

    for (TestMergedStreamService service : mergedStreamServices) {
      Thread.currentThread().setName(service.getName());
      service.prepareLastAddedPartitionMap();
      service.runPreExecute();
      service.runExecute();
      service.runPostExecute();
      service.clearPathPartitionTable();
    }

    LOG.info("Running MirrorStreamService Service");

    for (TestMirrorStreamService service : mirrorStreamServices) {
      Thread.currentThread().setName(service.getName());
      service.prepareLastAddedPartitionMap();
      service.runPreExecute();
      service.runExecute();
      service.runPostExecute();
      service.clearPathPartitionTable();
    }
    // clear all the inmemory static map values to avoid failures in other tests
    TestLocalStreamService.clearHCatInMemoryMaps();
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

    int i = 0;
    // Add a partition with (current -90 mins) timestamp in each table
    // (i.e. local, merge and mirror) for all streams
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.HOUR, -2);
    cal.add(Calendar.MINUTE, -30);
    lastAddedPartTime = cal.getTime();
    Map<String, String> partSpec = TestHCatUtil.getPartitionMap(cal);
    List<HCatFieldSchema> ptnCols = TestHCatUtil.getPartCols();
    Conduit.setHcatDBName(DB_NAME);
    
    for (String clusterName : clustersToProcess) {
      HCatClientUtil hcatClientUtil = hcatUtilList.get(i++);
      HCatClient hcatClient = TestHCatUtil.getHCatClient(hcatClientUtil);
      TestHCatUtil.createDataBase(DB_NAME, hcatClient);

      Cluster cluster = config.getClusters().get(clusterName);

      cluster.getHadoopConf().set("mapred.job.tracker",
          super.CreateJobConf().get("mapred.job.tracker"));
      TestLocalStreamService service = new TestLocalStreamPartition(config,
          cluster, currentCluster,new FSCheckpointProvider(cluster
              .getCheckpointDir()), streamsToProcessLocal, hcatClientUtil);
      String localrootDir = cluster.getLocalFinalDestDirRoot();
      for (String stream : streamsToProcessLocal) {
        String tableName = LOCAL_TABLE_NAME_PREFIX + "_" + stream;
        TestHCatUtil.createTable(hcatClient, DB_NAME, tableName, ptnCols);
        Path streamPath = new Path(localrootDir, stream);
        String location = CalendarHelper.getPathFromDate(lastAddedPartTime,
            streamPath).toString();
        LOG.info("add a partition to db: " + DB_NAME + " ,table: " + tableName
            + " ,partition location: " + location + " and partspecs:  " + partSpec);
        TestHCatUtil.addPartition(hcatClient, DB_NAME, tableName, location,
            partSpec);
      }
      localStreamServices.add(service);
      service.prepareLastAddedPartitionMap();
      service.getFileSystem().delete(
          new Path(service.getCluster().getRootDir()), true);
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
                        String clusterName1 = primaryCluster.getName();
            if (mirrorSrcClusterToStreamsMap.get(clusterName1) == null) {
              Set<String> tmp = new HashSet<String>();
              tmp.add(cStream.getName());
              mirrorSrcClusterToStreamsMap.put(clusterName1, tmp);
            } else {
              mirrorSrcClusterToStreamsMap.get(clusterName1).add(
                  cStream.getName());
            }
          }
        }
      }
      String destRootDir = cluster.getFinalDestDirRoot();
      for (String remote : mergedStreamRemoteClusters) {
        TestMergedStreamService remoteMergeService = new TestMergedStreamPartition(
            config, config.getClusters().get(remote), cluster, currentCluster,
            mergedSrcClusterToStreamsMap.get(remote), hcatClientUtil);
        mergedStreamServices.add(remoteMergeService);
        for (String stream : mergedSrcClusterToStreamsMap.get(remote)) {
          String tableName = TABLE_NAME_PREFIX + "_" + stream;
          try {
            TestHCatUtil.createTable(hcatClient, DB_NAME, tableName, ptnCols);
          } catch (HCatException e) {
            if (e.getCause() instanceof AlreadyExistsException) {
              LOG.warn(" table for merge stream was already created: ", e);
            } else {
              LOG.warn("Got exception while creating table for merge stream: ", e);
              return;
            }
          }
          Path streamPath = new Path(destRootDir, stream);
          String location = CalendarHelper.getPathFromDate(lastAddedPartTime,
              streamPath).toString();
          try {
            TestHCatUtil.addPartition(hcatClient, DB_NAME, tableName, location,
                partSpec);
          } catch (Exception e) {
            if (e.getCause() instanceof AlreadyExistsException) {
              LOG.warn("Partition " + partSpec + " already exists in the table :"
                  + " " + tableName );
            } else { 
              LOG.warn("Got exception while creating partition " + partSpec, e);
            }
          }
        }
        remoteMergeService.prepareLastAddedPartitionMap();
        if (currentCluster != null)
          Assert.assertEquals(remoteMergeService.getCurrentCluster(),
              currentCluster);
        else
          Assert.assertEquals(remoteMergeService.getCurrentCluster(), cluster);
      }

      for (String remote : mirroredRemoteClusters) {
        TestMirrorStreamService remoteMirrorService = new TestMirrorStreamPartition(
            config, config.getClusters().get(remote), cluster, currentCluster,
            mirrorSrcClusterToStreamsMap.get(remote), hcatClientUtil);
        mirrorStreamServices.add(remoteMirrorService);
        for (String stream : mirrorSrcClusterToStreamsMap.get(remote)) {
          String tableName = TABLE_NAME_PREFIX + "_" + stream;
          try {
            TestHCatUtil.createTable(hcatClient, DB_NAME, tableName, ptnCols);
          } catch (HCatException e) {
            if (e.getCause() instanceof AlreadyExistsException) {
              LOG.warn(" table for mirror stream was already created: ", e);
            } else {
              LOG.error("Got exception while creating table for mirror stream: ", e);
            }
          }
          Path streamPath = new Path(destRootDir, stream);
          String location = CalendarHelper.getPathFromDate(lastAddedPartTime,
              streamPath).toString();
          try {
            TestHCatUtil.addPartition(hcatClient, DB_NAME, tableName, location,
                partSpec);
          } catch (Exception e) {
            if (e.getCause() instanceof AlreadyExistsException) {
              LOG.warn("Partition " + partSpec + " already exists in the table :"
                  + " " + tableName );
            } else { 
              LOG.warn("Got exception while creating partition " + partSpec, e);
              return;
            }
          }
        }
        remoteMirrorService.prepareLastAddedPartitionMap();
        if (currentCluster != null)
          Assert.assertEquals(remoteMirrorService.getCurrentCluster(),
              currentCluster);
        else
          Assert.assertEquals(remoteMirrorService.getCurrentCluster(), cluster);
      }
      TestHCatUtil.submitBack(hcatClientUtil, hcatClient);
    }
  }*/
}