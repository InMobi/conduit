package com.inmobi.conduit;


import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.LogFactoryImpl;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.conduit.Cluster;
import com.inmobi.conduit.Conduit;
import com.inmobi.conduit.ConduitConfig;
import com.inmobi.conduit.ConduitConfigParser;
import com.inmobi.conduit.FSCheckpointProvider;
import com.inmobi.conduit.SourceStream;
import com.inmobi.conduit.TestMiniClusterUtil;
import com.inmobi.conduit.local.TestLocalStreamService;
import com.inmobi.conduit.utils.CalendarHelper;
import com.inmobi.conduit.utils.TestHCatUtil;


public class TestHCatPartitionMethods extends TestMiniClusterUtil {

  private static final Log LOG = LogFactoryImpl.getLog(TestHCatPartitionMethods.class);

  private static final int msPort = 20102;
  private String dbName = null;
  private Table table;
  private Set<TestLocalStreamService> services = new HashSet<TestLocalStreamService>();
  private Set<String> streamsToProcess = new HashSet<String>();

  @BeforeTest
  public void setup() throws Exception {
    System.setProperty(ConduitConstants.AUDIT_ENABLED_KEY, "true");
    super.setup(2, 6, 1);
    Conduit.setHCatEnabled(true);
    Conduit.setHcatDBName("conduit");
    dbName = Conduit.getHcatDBName();
    HiveConf hiveConf = TestHCatUtil.getHcatConf(msPort,
        "target/metastore-unittest", "metadb-unittest");
    TestHCatUtil.startMetaStoreServer(hiveConf, msPort);
    Conduit.setHiveConf(hiveConf);
    parseConduit();
  }

  @AfterTest
  public void cleanup() throws Exception {
    Conduit.setHCatEnabled(false);
    for (TestLocalStreamService service: services) {
      clearInMemoryMaps(service);
    }
    TestHCatUtil.stop();
    super.cleanup();
  }

  private void parseConduit() throws Exception {
    ConduitConfigParser parser = new ConduitConfigParser("test-lss-hcat-conduit-test.xml");
    ConduitConfig config = parser.getConfig();
    streamsToProcess.addAll(config.getSourceStreams().keySet());
    Set<String> clustersToProcess = new HashSet<String>();

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

  }

  @Test
  public void testFindLastPartition() throws Exception {
    TestHCatUtil thutil = new TestHCatUtil();
    Conduit.setHcatDBName("conduit1");
    dbName = Conduit.getHcatDBName();
    TestHCatUtil.createDatabase(dbName);
    for (TestLocalStreamService service : services) {
      for (String stream : streamsToProcess) {
        String tableName = "conduit_local_" + stream;
        try {
          table = thutil.createTable(Conduit.getHcatDBName(), tableName);
        } catch (HiveException e) {
          if (e.getCause() instanceof AlreadyExistsException) {
            LOG.warn("Table " + tableName + "already exists ");
          }
        }
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.MINUTE, -10);
        cal.set(Calendar.MILLISECOND, 0);
        cal.set(Calendar.SECOND, 0);
        Date currentTime = cal.getTime();
        cal.add(Calendar.HOUR_OF_DAY, -1);
        cal.add(Calendar.MINUTE, -20);
        String rootPath= service.getCluster().getLocalFinalDestDirRoot();
        Path pathPrefix = new Path(rootPath, stream);
        service.prepareLastAddedPartitionMap();
        service.findLastPartition(stream);
        // No partitions present in the hcatalog table
        Assert.assertEquals(service.getLastAddedPartTime(tableName), -1);
        while (!cal.getTime().after(currentTime)) {
          Path datePath = CalendarHelper.getPathFromDate(cal.getTime(),
              pathPrefix);
          LOG.info("Adding partition for path " + TestHCatUtil.getPartitionMap(cal) + " datePath " + datePath);
          TestHCatUtil.addPartition(table, TestHCatUtil.getPartitionMap(cal));
          cal.add(Calendar.MINUTE, 1);
        }
        service.findLastPartition(stream);
        long lastAddedValue = service.getLastAddedPartTime(tableName);
        Assert.assertEquals(lastAddedValue, currentTime.getTime());
        service.getFileSystem().delete(
            new Path(service.getCluster().getRootDir()), true);
        clearInMemoryMaps(service);
      }
    }
  }

  private void clearInMemoryMaps(TestLocalStreamService service) {
    service.clearPathPartitionTable();
    service.clearHCatInMemoryMaps();
  }

  @Test
  public void testFindDiffBetweenLastAddedPartTimeAndFirstPath() throws Exception {
    for (TestLocalStreamService service : services) {
      for (String stream : streamsToProcess) {
        service.getFileSystem().delete(
            new Path(service.getCluster().getRootDir()), true);
        String tableName = "conduit_local_" + stream;
        Calendar cal1 = Calendar.getInstance();
        Date currentTime1 = cal1.getTime();
        cal1.add(Calendar.MINUTE, -10);
        cal1.set(Calendar.MILLISECOND, 0);
        cal1.set(Calendar.SECOND, 0);
        String timeString = CalendarHelper.minDirFormat.get().format(currentTime1);
        String pathPrefix = new Path(service.getCluster().
            getLocalFinalDestDirRoot(), stream).toString();
        Path currentMinDir = new Path(pathPrefix, timeString);
        service.prepareLastAddedPartitionMap();
        service.updateLastAddedPartitionMap(tableName, cal1.getTime().getTime());
        Set<Path> pathsList = new TreeSet<Path>();
        pathsList.add(currentMinDir);
        TestLocalStreamService.pathsToBeregisteredPerTable.put(tableName, pathsList);
        service.findDiffBetweenLastAddedAndFirstPath(stream, tableName);
        Assert.assertTrue(TestLocalStreamService.pathsToBeregisteredPerTable.
            get(tableName).size() == 10);
        service.getFileSystem().delete(
            new Path(service.getCluster().getRootDir()), true);
        clearInMemoryMaps(service);
      }
    }
  }

  @Test
  public void testLocalStreamServiceWithLastAddedPartTime() throws Exception {
    TestHCatUtil thutil = new TestHCatUtil();
    Conduit.setHcatDBName("conduit2");
    Conduit.setHCatEnabled(true);
    dbName = Conduit.getHcatDBName();
    TestHCatUtil.createDatabase(dbName);
    for (TestLocalStreamService service : services) {
      for (String stream : streamsToProcess) {
        service.getFileSystem().delete(
            new Path(service.getCluster().getRootDir()), true);
        String tableName = "conduit_local_" + stream;
        table = thutil.createTable(Conduit.getHcatDBName(), tableName);

        Calendar cal1 = Calendar.getInstance();
        Date currentTime1 = cal1.getTime();
        cal1.add(Calendar.HOUR_OF_DAY, -2);
        cal1.add(Calendar.MINUTE, -30);
        cal1.set(Calendar.MILLISECOND, 0);
        cal1.set(Calendar.SECOND, 0);
        String timeString = CalendarHelper.minDirFormat.get().format(currentTime1);
        String pathPrefix = new Path(service.getCluster().getLocalFinalDestDirRoot(), stream).toString();
        Path currentMinDir = new Path(pathPrefix, timeString);
        LOG.info("Adding partition for path " + TestHCatUtil.getPartitionMap(cal1));
        TestHCatUtil.addPartition(table, TestHCatUtil.getPartitionMap(cal1));

        service.prepareLastAddedPartitionMap();
        service.runPreExecute();
        service.runExecute();
        List<Partition> partitionList = Hive.get().getPartitions(table);
        /* last added partition is 150 mins back. For the latest minute,
         * partition won't be created immediately. So, it should have at least
         * 149 partitions
         */
        LOG.info("Number of partitions in the hcat table : " + partitionList.size() + ". Partitions are " + partitionList);
        Assert.assertTrue(partitionList.size() >= 149);
        clearInMemoryMaps(service);
        service.getFileSystem().delete(
            new Path(service.getCluster().getRootDir()), true);
      }
    }
  }
}
