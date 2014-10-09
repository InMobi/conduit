package com.inmobi.conduit.local;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.inmobi.conduit.AbstractService;
import com.inmobi.conduit.Cluster;
import com.inmobi.conduit.Conduit;
import com.inmobi.conduit.ConduitConfigParser;
import com.inmobi.conduit.ConduitConstants;
import com.inmobi.conduit.FSCheckpointProvider;
import com.inmobi.conduit.metrics.AbsoluteGauge;
import com.inmobi.conduit.metrics.ConduitMetrics;
import com.inmobi.conduit.utils.FileUtil;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.util.AuditUtil;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class TestLocalLastFileProcessed {
  private static Logger LOG = Logger.getLogger(TestLocalLastFileProcessed.class);

  private static FileSystem localFs;
  private static final String checkpointDir =
      "/tmp/test-conduit/conduit-lss-checkpoint";
  private static Cluster cluster;
  private static ConduitConfigParser parser;
  private static String stream1, stream2;

  @BeforeTest
  public void setup() throws Exception {
    System.setProperty(Conduit.AUDIT_ENABLED_KEY, "false");
    localFs = FileSystem.getLocal(new Configuration());
    parser = new ConduitConfigParser("src/test/resources/test-lss-conduit1.xml");
    // Copy input format src jar to FS
    String inputFormatSrcJar = FileUtil
        .findContainingJar(com.inmobi.conduit.distcp.tools.mapred.UniformSizeInputFormat.class);
    cluster = parser.getConfig().getClusters().get("testcluster1");
    Path jarsPath = new Path(cluster.getTmpPath(), "jars");
    localFs.copyFromLocalFile(new Path(inputFormatSrcJar),
        new Path(jarsPath, "conduit-distcp-current.jar"));
    String auditSrcJar = FileUtil.findContainingJar(
        com.inmobi.messaging.util.AuditUtil.class);
    localFs.copyFromLocalFile(new Path(auditSrcJar),
        new Path(jarsPath, "messaging-client-core.jar"));

    Iterator<String> setIterator = cluster.getSourceStreams().iterator();
    if (setIterator.hasNext()) {
      stream1 = setIterator.next();
    }
    if (setIterator.hasNext()) {
      stream2 = setIterator.next();
    }
  }

  @BeforeMethod
  public void setupBeforeMethod() throws Exception {
    Properties prop = new Properties();
    prop.setProperty("com.inmobi.conduit.metrics.enabled", "true");
    prop.setProperty("com.inmobi.conduit.metrics.slidingwindowtime", "100000000");
    ConduitMetrics.init(prop);
    ConduitMetrics.startAll();
  }

  @AfterMethod
  public void cleanupAftMethod() throws Exception {
    ConduitMetrics.stopAll();
    cleanup();
  }

  @AfterTest
  public void cleanup() throws Exception {
    localFs.delete(cluster.getDataDir(), true);
    localFs.delete(new Path(checkpointDir), true);
  }

  private Long getMinute(Long time) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTimeInMillis(time);
    calendar.set(Calendar.MILLISECOND, 0);
    calendar.set(Calendar.SECOND, 0);
    return calendar.getTimeInMillis();
  }

  private void createFiles(Calendar calendar, String streamName,
      String collectorName, int numOfFiles, boolean isDataFile)
          throws IOException, InterruptedException {
    String path = cluster.getDataDir() + File.separator + streamName +
        File.separator + collectorName + File.separator;
    localFs.mkdirs(new Path(path));
    for (int i = 0; i < numOfFiles; i++) {
      calendar.add(Calendar.MINUTE, 1);
      String filenameStr = streamName + "-" + TestLocalStreamService
          .getDateAsYYYYMMDDHHmm(calendar.getTime()) + "_" +
          TestLocalStreamService.idFormat.format(0);
      LOG.debug("Creating Test Data with filename [" + filenameStr + "]");
      FSDataOutputStream streamout = localFs.create(new Path(path, filenameStr));
      if (isDataFile) {
        String content = "Creating Test data for teststream";
        Message msg = new Message(content.getBytes());
        long currentTimestamp = new Date().getTime();
        AuditUtil.attachHeaders(msg, currentTimestamp);
        byte[] encodeMsg = Base64.encodeBase64(msg.getData().array());
        streamout.write(encodeMsg);
      }
      streamout.close();
      Thread.sleep(1000);
    }
  }

  @Test
  public void testEmptyDirectory() throws Exception {
    String path1 = cluster.getDataDir() + File.separator + stream1 +
        File.separator + cluster.getName() + File.separator;
    String path2 = cluster.getDataDir() + File.separator + stream2 +
        File.separator + cluster.getName() + File.separator;
    localFs.mkdirs(new Path(path1));
    localFs.mkdirs(new Path(path2));
    LocalStreamService service = new LocalStreamService(parser.getConfig(),
        cluster, null, new FSCheckpointProvider(checkpointDir),
        cluster.getSourceStreams());
    service.execute();
    Assert.assertEquals(0, ConduitMetrics.<AbsoluteGauge>getMetric(service
        .getServiceType(), AbstractService.LAST_FILE_PROCESSED,
        stream1).getValue());
    Assert.assertEquals(0, ConduitMetrics.<AbsoluteGauge>getMetric(service
        .getServiceType(), AbstractService.LAST_FILE_PROCESSED,
        stream2).getValue());
  }

  @Test
  public void testAllEmptyFiles() throws Exception {
    int numFiles = 9;
    Calendar calendar = Calendar.getInstance();
    calendar.add(Calendar.MINUTE, -10);
    Date firstDate = calendar.getTime();

    createFiles(calendar, stream1, cluster.getName(), numFiles, false);
    long lastAddedDateStream1 = getMinute(calendar.getTimeInMillis() - 60000);

    calendar.setTime(firstDate);
    createFiles(calendar, stream2, cluster.getName(), numFiles, false);

    long lastAddedDateStream2 = getMinute(calendar.getTimeInMillis() - 60000);

    LocalStreamService service = new LocalStreamService(parser.getConfig(),
        cluster, null, new FSCheckpointProvider(checkpointDir),
        cluster.getSourceStreams());
    service.execute();
    createFiles(calendar, stream1, cluster.getName(), numFiles, false);
    Assert.assertEquals(lastAddedDateStream1, ConduitMetrics.<AbsoluteGauge
        >getMetric(
            service.getServiceType(), AbstractService.LAST_FILE_PROCESSED,
            stream1).getValue().longValue());
    Assert.assertEquals(lastAddedDateStream2, ConduitMetrics
        .<AbsoluteGauge>getMetric(
            service.getServiceType(), AbstractService.LAST_FILE_PROCESSED,
            stream2).getValue().longValue());
    long lastAddedDateStream3 = getMinute(calendar.getTimeInMillis() - 60000);
    service.execute();
    Assert.assertEquals(lastAddedDateStream3, ConduitMetrics.<AbsoluteGauge
        >getMetric(
            service.getServiceType(), AbstractService.LAST_FILE_PROCESSED,
            stream1).getValue().longValue());
  }

  @Test
  public void testAllNonEmptyFiles() throws Exception {
    int numFiles = 6;
    Calendar calendar = Calendar.getInstance();
    calendar.add(Calendar.MINUTE, -7);

    createFiles(calendar, stream1, cluster.getName(), numFiles, true);
    Long lastAddedDateStream1 = getMinute(calendar.getTimeInMillis() - 60000);

    createFiles(calendar, stream2, cluster.getName(), numFiles, true);
    Long lastAddedDateStream2 = getMinute(calendar.getTimeInMillis() - 60000);

    Map<FileStatus, String> results = new HashMap<FileStatus, String>();
    Set<FileStatus> trashSet = new HashSet<FileStatus>();
    Table<String, String, String> checkpointPaths = HashBasedTable.create();
    TestLocalStreamService service = new TestLocalStreamService(parser.getConfig(),
        cluster, null, new FSCheckpointProvider(checkpointDir),
        cluster.getSourceStreams(), null);
    service.createListing(localFs,  localFs.getFileStatus(cluster.getDataDir
        ()), results, trashSet, checkpointPaths);
    Assert.assertEquals(lastAddedDateStream1, service.getLastProcessedMap()
        .get(stream1));
    Assert.assertEquals(lastAddedDateStream2, service.getLastProcessedMap()
        .get(stream2));
  }

  @Test
  public void testAtleastOneNonEmptyFile() throws Exception {
    int numFiles = 6;
    Calendar calendar = Calendar.getInstance();
    calendar.add(Calendar.MINUTE, -7);

    createFiles(calendar, stream1, cluster.getName(), numFiles - 2, false);
    createFiles(calendar, stream1, cluster.getName(), 2, true);

    Long lastAddedDateStream1 = getMinute(calendar.getTimeInMillis() - 60000);
    Set<String> newStreamToProcess = new HashSet<String>();
    newStreamToProcess.add(stream1);

    Map<FileStatus, String> results = new HashMap<FileStatus, String>();
    Set<FileStatus> trashSet = new HashSet<FileStatus>();
    Table<String, String, String> checkpointPaths = HashBasedTable.create();
    TestLocalStreamService service = new TestLocalStreamService(parser.getConfig(),
        cluster, null, new FSCheckpointProvider(checkpointDir),
        newStreamToProcess, null);
    service.createListing(localFs, localFs.getFileStatus(cluster.getDataDir
        ()), results, trashSet, checkpointPaths);
    Assert.assertEquals(lastAddedDateStream1, service.getLastProcessedMap()
        .get(stream1));
  }

  @Test
  public void testMultipleCollectorsAllEmptyFiles() throws Exception {
    int numFiles = 6;
    Calendar calendar = Calendar.getInstance();
    calendar.add(Calendar.MINUTE, -7);

    createFiles(calendar, stream1, cluster.getName(), numFiles, false);
    long lastFileAddedInFirstCollector = getMinute(calendar.getTimeInMillis() - 60000);

    createFiles(calendar, stream1, cluster.getName() + "_1", numFiles, false);

    Set<String> newStreamToProcess = new HashSet<String>();
    newStreamToProcess.add(stream1);

    LocalStreamService service = new LocalStreamService(parser.getConfig(),
        cluster, null, new FSCheckpointProvider(checkpointDir),
        newStreamToProcess);
    service.execute();
    /*
     * Value of metric is minimum(latest file time stamp for each collector).
     */
    Assert.assertEquals(lastFileAddedInFirstCollector, ConduitMetrics
        .<AbsoluteGauge>getMetric(
            service.getServiceType(), AbstractService.LAST_FILE_PROCESSED,
            stream1).getValue().longValue());
  }

  @Test
  public void testMultipleCollectorsOneNonEmptyFiles() throws Exception {
    int numFiles = 6;
    Calendar calendar = Calendar.getInstance();
    calendar.add(Calendar.MINUTE, -7);
    Date firstDate = calendar.getTime();

    createFiles(calendar, stream1, cluster.getName(), numFiles - 2, false);
    createFiles(calendar, stream1, cluster.getName(), 2, true);
    Long latestFileTimeStampInFirstCollector = getMinute(calendar.getTimeInMillis() - 60000);

    calendar.setTime(firstDate);
    createFiles(calendar, stream1, cluster.getName() + "_1", numFiles, false);
    Long latestFileTimeStampInSecondCollector = getMinute(calendar.getTimeInMillis() - 60000);

    Long fileTimeStamp = (latestFileTimeStampInFirstCollector < latestFileTimeStampInSecondCollector) ?
        latestFileTimeStampInFirstCollector : latestFileTimeStampInSecondCollector;

    Set<String> newStreamToProcess = new HashSet<String>();
    newStreamToProcess.add(stream1);

    Map<FileStatus, String> results = new HashMap<FileStatus, String>();
    Set<FileStatus> trashSet = new HashSet<FileStatus>();
    Table<String, String, String> checkpointPaths = HashBasedTable.create();
    TestLocalStreamService service = new TestLocalStreamService(parser.getConfig(),
        cluster, null, new FSCheckpointProvider(checkpointDir),
        newStreamToProcess, null);
    service.createListing(localFs, localFs.getFileStatus(cluster.getDataDir
        ()), results, trashSet, checkpointPaths);
    Assert.assertEquals(fileTimeStamp, service.getLastProcessedMap().get(stream1));
  }

  @Test
  public void testMultipleCollectorsAllAtleastOneNonEmptyFiles() throws Exception {
    int numFiles = 6;
    Calendar calendar = Calendar.getInstance();
    calendar.add(Calendar.MINUTE, -7);
    Date firstDate = calendar.getTime();

    createFiles(calendar, stream1, cluster.getName(), 2, false);
    createFiles(calendar, stream1, cluster.getName(), 2, true);
    Long latestFileTimeStampInFirstCollector = getMinute(calendar.getTimeInMillis() - 60000);

    calendar.setTime(firstDate);
    createFiles(calendar, stream1, cluster.getName() + "_1", 2, true);
    createFiles(calendar, stream1, cluster.getName() + "_1", numFiles - 2, false);

    Long latestFileTimeStampInsecondCollector = getMinute(calendar.getTimeInMillis() - 60000);

    Long fileTimeStamp = (latestFileTimeStampInFirstCollector < latestFileTimeStampInsecondCollector) ?
        latestFileTimeStampInFirstCollector : latestFileTimeStampInsecondCollector;

    Set<String> newStreamToProcess = new HashSet<String>();
    newStreamToProcess.add(stream1);

    Map<FileStatus, String> results = new HashMap<FileStatus, String>();
    Set<FileStatus> trashSet = new HashSet<FileStatus>();
    Table<String, String, String> checkpointPaths = HashBasedTable.create();
    TestLocalStreamService service = new TestLocalStreamService(parser.getConfig(),
        cluster, null, new FSCheckpointProvider(checkpointDir),
        newStreamToProcess, null);
    service.createListing(localFs, localFs.getFileStatus(cluster.getDataDir
        ()), results, trashSet, checkpointPaths);
    Assert.assertEquals(fileTimeStamp,
        service.getLastProcessedMap().get(stream1));
  }
}
