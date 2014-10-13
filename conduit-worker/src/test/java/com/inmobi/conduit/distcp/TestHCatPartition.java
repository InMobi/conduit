package com.inmobi.conduit.distcp;

import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.LogFactoryImpl;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hive.hcatalog.api.HCatClient;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.conduit.Cluster;
import com.inmobi.conduit.Conduit;
import com.inmobi.conduit.ConduitConfig;
import com.inmobi.conduit.ConduitConfigParser;
import com.inmobi.conduit.FSCheckpointProvider;
import com.inmobi.conduit.HCatClientUtil;
import com.inmobi.conduit.SourceStream;
import com.inmobi.conduit.TestMiniClusterUtil;
import com.inmobi.conduit.local.TestLocalStreamService;
import com.inmobi.conduit.utils.CalendarHelper;
import com.inmobi.conduit.utils.TestHCatUtil;

public class TestHCatPartition extends TestMiniClusterUtil{

  private static final Log LOG = LogFactoryImpl.getLog(TestHCatPartition.class);

  private static final int msPort = 20100;
  private HCatClientUtil hcatClientUtil = null;
  private String dbName = null;

  @BeforeTest
  public void setup() throws Exception {
    super.setup(2, 6, 1);
    Conduit.setHCatEnabled(true);
    Conduit.setHcatDBName("conduit");
    dbName = Conduit.getHcatDBName();
    HiveConf hiveConf = TestHCatUtil.getHcatConf(msPort,
        "target/metastore-unittest", "metadb-unittest");
    TestHCatUtil.startMetaStoreServer(hiveConf, msPort);
   // hcatClientUtil = TestHCatUtil.getHCatUtil(hiveConf);
    TestHCatUtil.createHCatClients(hiveConf, hcatClientUtil);
    HCatClient hcatClient = TestHCatUtil.getHCatClient(hcatClientUtil);
    // LOG.info("AAAAAAAAAAAAA hcat client ");
    TestHCatUtil.createDataBase(dbName, hcatClient);
    TestHCatUtil.submitBack(hcatClientUtil, hcatClient);
  }

  public void cleanup() throws Exception {
    if (hcatClientUtil != null) {
      hcatClientUtil.close();
    }
    super.cleanup();
  }

  @Test
  public void testFindLastPartition() throws Exception {
    ConduitConfigParser parser = new ConduitConfigParser("test-lss-hcat-conduit-test.xml");
    ConduitConfig config = parser.getConfig();
    Set<String> streamsToProcess = new HashSet<String>();
    streamsToProcess.addAll(config.getSourceStreams().keySet());
    Set<String> clustersToProcess = new HashSet<String>();
    Set<TestLocalStreamService> services = new HashSet<TestLocalStreamService>();

    for (SourceStream sStream : config.getSourceStreams().values()) {
      for (String cluster : sStream.getSourceClusters()) {
        clustersToProcess.add(cluster);
      }
    }

    for (String clusterName : clustersToProcess) {
      Cluster cluster = config.getClusters().get(clusterName);
      cluster.getHadoopConf().set("mapred.job.tracker",
          super.CreateJobConf().get("mapred.job.tracker"));
      TestLocalStreamService service = new TestLocalStreamService(config,
          cluster, null, new FSCheckpointProvider(cluster.getCheckpointDir()),
          streamsToProcess);
      services.add(service);
      service.getFileSystem().delete(
          new Path(service.getCluster().getRootDir()), true);
    }
    for (TestLocalStreamService service : services) {
      HCatClient hcatClient = TestHCatUtil.getHCatClient(hcatClientUtil);
      for (String stream : streamsToProcess) {
//        if (hcatClient != null) {
          String tableName = "conduit_local_" + stream;
  //        TestHCatUtil.createTable(hcatClient, dbName, tableName,
     //         TestHCatUtil.getPartCols());
          //Hive.get().createTable(tbl);
          Calendar cal = Calendar.getInstance();
          cal.add(Calendar.MINUTE, -10);
          cal.set(Calendar.MILLISECOND, 0);
          cal.set(Calendar.SECOND, 0);
          Date currentTime = cal.getTime();
          cal.add(Calendar.MINUTE, -20);
          String rootPath= service.getCluster().getLocalFinalDestDirRoot();
          Path pathPrefix = new Path(rootPath, stream);
          service.findLastPartition(stream);
          // No partitions present in the hcatalog table
          Assert.assertEquals(service.getLastAddedPartTime(tableName), -1);
          while (!cal.getTime().after(currentTime)) {
            Path datePath = CalendarHelper.getPathFromDate(cal.getTime(),
                pathPrefix);
            LOG.info("Adding partition for path " + datePath);
            TestHCatUtil.addPartition(hcatClient, dbName, tableName,
                datePath.toString(), TestHCatUtil.getPartitionMap(cal));
            cal.add(Calendar.MINUTE, 1);
          }
          service.findLastPartition(stream);
          long lastAddedValue = service.getLastAddedPartTime(tableName);
          Assert.assertEquals(lastAddedValue, currentTime.getTime());
        }
      }
    }
 // }
}
