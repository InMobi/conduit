package com.inmobi.conduit.local;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.inmobi.conduit.AbstractService;
import com.inmobi.conduit.Cluster;
import com.inmobi.conduit.ConduitConfigParser;
import com.inmobi.conduit.FSCheckpointProvider;
import com.inmobi.conduit.metrics.AbsoluteGauge;
import com.inmobi.conduit.metrics.ConduitMetrics;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.util.AuditUtil;
import junit.framework.Assert;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.testng.annotations.*;

import java.io.File;
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
    localFs = FileSystem.getLocal(new Configuration());
    parser = new ConduitConfigParser("src/test/resources/test-lss-conduit1.xml");
    cluster = parser.getConfig().getClusters().get("testcluster1");
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
    int numFiles = 6;
    Calendar calendar = Calendar.getInstance();
    calendar.add(Calendar.MINUTE, -7);
    Date firstDate = calendar.getTime();

    String path1 = cluster.getDataDir() + File.separator + stream1 +
        File.separator + cluster.getName() + File.separator;
    localFs.mkdirs(new Path(path1));
    long firstAddedDateStream1 = getMinute(calendar.getTimeInMillis() + 60000);
    for (int i = 0; i < numFiles; i++) {
      calendar.add(Calendar.MINUTE, 1);
      String filenameStr = stream1 + "-" + TestLocalStreamService
          .getDateAsYYYYMMDDHHmm(calendar.getTime()) + "_" +
          TestLocalStreamService.idFormat.format(0);
      LOG.debug("Creating Test Data with filename [" + filenameStr + "]");
      FSDataOutputStream streamout = localFs.create(new Path(path1,
          filenameStr));
      streamout.close();
      Thread.sleep(1000);
    }

    String path2 = cluster.getDataDir() + File.separator + stream2 +
        File.separator + cluster.getName() + File.separator;
    localFs.mkdirs(new Path(path2));
    calendar.setTime(firstDate);
    long firstAddedDateStream2 = getMinute(calendar.getTimeInMillis() + 60000);
    for (int i = 0; i < numFiles; i++) {
      calendar.add(Calendar.MINUTE, 1);
      String filenameStr = stream2 + "-" + TestLocalStreamService
          .getDateAsYYYYMMDDHHmm(calendar.getTime()) + "_" +
          TestLocalStreamService.idFormat.format(0);
      LOG.debug("Creating Test Data with filename [" + filenameStr + "]");
      FSDataOutputStream streamout = localFs.create(new Path(path2,
          filenameStr));
      streamout.close();
      Thread.sleep(1000);
    }

    LocalStreamService service = new LocalStreamService(parser.getConfig(),
        cluster, null, new FSCheckpointProvider(checkpointDir),
        cluster.getSourceStreams());
    service.execute();
    Assert.assertEquals(firstAddedDateStream1, ConduitMetrics.<AbsoluteGauge
        >getMetric(
        service.getServiceType(), AbstractService.LAST_FILE_PROCESSED,
        stream1).getValue().longValue());
    Assert.assertEquals(firstAddedDateStream2, ConduitMetrics
        .<AbsoluteGauge>getMetric(
            service.getServiceType(), AbstractService.LAST_FILE_PROCESSED,
            stream2).getValue().longValue());
  }

  @Test
  public void testAllNonEmptyFiles() throws Exception {
    int numFiles = 6;
    Calendar calendar = Calendar.getInstance();
    calendar.add(Calendar.MINUTE, -7);
    Date firstDate = calendar.getTime();

    String path1 = cluster.getDataDir() + File.separator + stream1 +
        File.separator + cluster.getName() + File.separator;
    localFs.mkdirs(new Path(path1));
    for (int i = 0; i < numFiles; i++) {
      calendar.add(Calendar.MINUTE, 1);
      String filenameStr = stream1 + "-" + TestLocalStreamService
          .getDateAsYYYYMMDDHHmm(calendar.getTime()) + "_" +
          TestLocalStreamService.idFormat.format(0);
      LOG.debug("Creating Test Data with filename [" + filenameStr + "]");
      FSDataOutputStream streamout = localFs.create(new Path(path1,
          filenameStr));
      String content = "Creating Test data for teststream";
      Message msg = new Message(content.getBytes());
      long currentTimestamp = new Date().getTime();
      AuditUtil.attachHeaders(msg, currentTimestamp);
      byte[] encodeMsg = Base64.encodeBase64(msg.getData().array());
      streamout.write(encodeMsg);
      streamout.close();
      Thread.sleep(1000);
    }
    Long lastAddedDateStream1 = getMinute(firstDate.getTime() + 60000);

    String path2 = cluster.getDataDir() + File.separator + stream2 +
        File.separator + cluster.getName() + File.separator;
    localFs.mkdirs(new Path(path2));
    Date secondDate = calendar.getTime();
    for (int i = 0; i < numFiles; i++) {
      calendar.add(Calendar.MINUTE, 1);
      String filenameStr = stream2 + "-" + TestLocalStreamService
          .getDateAsYYYYMMDDHHmm(calendar.getTime()) + "_" +
          TestLocalStreamService.idFormat.format(0);
      LOG.debug("Creating Test Data with filename [" + filenameStr + "]");
      FSDataOutputStream streamout = localFs.create(new Path(path2,
          filenameStr));
      String content = "Creating Test data for teststream";
      Message msg = new Message(content.getBytes());
      long currentTimestamp = new Date().getTime();
      AuditUtil.attachHeaders(msg, currentTimestamp);
      byte[] encodeMsg = Base64.encodeBase64(msg.getData().array());
      streamout.write(encodeMsg);
      streamout.close();
      Thread.sleep(1000);
    }
    Long lastAddedDateStream2 = getMinute(secondDate.getTime() + 60000);

    Map<FileStatus, String> results = new HashMap<FileStatus, String>();
    Set<FileStatus> trashSet = new HashSet<FileStatus>();
    Table<String, String, String> checkpointPaths = HashBasedTable.create();
    TestLocalStreamService service = new TestLocalStreamService(parser.getConfig(),
        cluster, null, new FSCheckpointProvider(checkpointDir),
        cluster.getSourceStreams());
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

    String path1 = cluster.getDataDir() + File.separator + stream1 +
        File.separator + cluster.getName() + File.separator;
    localFs.mkdirs(new Path(path1));
    for (int i = 0; i < numFiles - 2; i++) {
      calendar.add(Calendar.MINUTE, 1);
      String filenameStr = stream1 + "-" + TestLocalStreamService
          .getDateAsYYYYMMDDHHmm(calendar.getTime()) + "_" +
          TestLocalStreamService.idFormat.format(0);
      LOG.debug("Creating Test Data with filename [" + filenameStr + "]");
      FSDataOutputStream streamout = localFs.create(new Path(path1,
          filenameStr));
      streamout.close();
      Thread.sleep(1000);
    }
    for (int i = 0; i < 2; i++) {
      calendar.add(Calendar.MINUTE, 1);
      String filenameStr = stream1 + "-" + TestLocalStreamService
          .getDateAsYYYYMMDDHHmm(calendar.getTime()) + "_" +
          TestLocalStreamService.idFormat.format(0);
      LOG.debug("Creating Test Data with filename [" + filenameStr + "]");
      FSDataOutputStream streamout = localFs.create(new Path(path1,
          filenameStr));
      String content = "Creating Test data for teststream";
      Message msg = new Message(content.getBytes());
      long currentTimestamp = new Date().getTime();
      AuditUtil.attachHeaders(msg, currentTimestamp);
      byte[] encodeMsg = Base64.encodeBase64(msg.getData().array());
      streamout.write(encodeMsg);
      streamout.close();
      Thread.sleep(1000);
    }
    Long lastAddedDateStream1 = getMinute(calendar.getTimeInMillis() - 60000);
    Set<String> newStreamToProcess = new HashSet<String>();
    newStreamToProcess.add(stream1);

    Map<FileStatus, String> results = new HashMap<FileStatus, String>();
    Set<FileStatus> trashSet = new HashSet<FileStatus>();
    Table<String, String, String> checkpointPaths = HashBasedTable.create();
    TestLocalStreamService service = new TestLocalStreamService(parser.getConfig(),
        cluster, null, new FSCheckpointProvider(checkpointDir),
        newStreamToProcess);
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

    String path1 = cluster.getDataDir() + File.separator + stream1 +
        File.separator + cluster.getName() + File.separator;
    localFs.mkdirs(new Path(path1));
    long firstAddedDateStream = getMinute(calendar.getTimeInMillis() + 60000);
    for (int i = 0; i < numFiles; i++) {
      calendar.add(Calendar.MINUTE, 1);
      String filenameStr = stream1 + "-" + TestLocalStreamService
          .getDateAsYYYYMMDDHHmm(calendar.getTime()) + "_" +
          TestLocalStreamService.idFormat.format(0);
      LOG.debug("Creating Test Data with filename [" + filenameStr + "]");
      FSDataOutputStream streamout = localFs.create(new Path(path1,
          filenameStr));
      streamout.close();
      Thread.sleep(1000);
    }

    String path2 = cluster.getDataDir() + File.separator + stream1 +
        File.separator + "testcollector" + File.separator;
    localFs.mkdirs(new Path(path2));
    for (int i = 0; i < numFiles; i++) {
      calendar.add(Calendar.MINUTE, 1);
      String filenameStr = stream1 + "-" + TestLocalStreamService
          .getDateAsYYYYMMDDHHmm(calendar.getTime()) + "_" +
          TestLocalStreamService.idFormat.format(0);
      LOG.debug("Creating Test Data with filename [" + filenameStr + "]");
      FSDataOutputStream streamout = localFs.create(new Path(path2,
          filenameStr));
      streamout.close();
      Thread.sleep(1000);
    }

    Set<String> newStreamToProcess = new HashSet<String>();
    newStreamToProcess.add(stream1);

    LocalStreamService service = new LocalStreamService(parser.getConfig(),
        cluster, null, new FSCheckpointProvider(checkpointDir),
        newStreamToProcess);
    service.execute();
    Assert.assertEquals(firstAddedDateStream, ConduitMetrics
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

    String path1 = cluster.getDataDir() + File.separator + stream1 +
        File.separator + cluster.getName() + File.separator;
    localFs.mkdirs(new Path(path1));
    for (int i = 0; i < numFiles - 2; i++) {
      calendar.add(Calendar.MINUTE, 1);
      String filenameStr = stream1 + "-" + TestLocalStreamService
          .getDateAsYYYYMMDDHHmm(calendar.getTime()) + "_" +
          TestLocalStreamService.idFormat.format(0);
      LOG.debug("Creating Test Data with filename [" + filenameStr + "]");
      FSDataOutputStream streamout = localFs.create(new Path(path1,
          filenameStr));
      streamout.close();
      Thread.sleep(1000);
    }
    for (int i = 0; i < 2; i++) {
      calendar.add(Calendar.MINUTE, 1);
      String filenameStr = stream1 + "-" + TestLocalStreamService
          .getDateAsYYYYMMDDHHmm(calendar.getTime()) + "_" +
          TestLocalStreamService.idFormat.format(0);
      LOG.debug("Creating Test Data with filename [" + filenameStr + "]");
      FSDataOutputStream streamout = localFs.create(new Path(path1,
          filenameStr));
      String content = "Creating Test data for teststream";
      Message msg = new Message(content.getBytes());
      long currentTimestamp = new Date().getTime();
      AuditUtil.attachHeaders(msg, currentTimestamp);
      byte[] encodeMsg = Base64.encodeBase64(msg.getData().array());
      streamout.write(encodeMsg);
      streamout.close();
      Thread.sleep(1000);
    }

    String path2 = cluster.getDataDir() + File.separator + stream1 +
        File.separator + "testcollector" + File.separator;
    localFs.mkdirs(new Path(path2));
    calendar.setTime(firstDate);
    for (int i = 0; i < numFiles; i++) {
      calendar.add(Calendar.MINUTE, 1);
      String filenameStr = stream1 + "-" + TestLocalStreamService
          .getDateAsYYYYMMDDHHmm(calendar.getTime()) + "_" +
          TestLocalStreamService.idFormat.format(0);
      LOG.debug("Creating Test Data with filename [" + filenameStr + "]");
      FSDataOutputStream streamout = localFs.create(new Path(path2,
          filenameStr));
      streamout.close();
      Thread.sleep(1000);
    }

    Set<String> newStreamToProcess = new HashSet<String>();
    newStreamToProcess.add(stream1);

    Map<FileStatus, String> results = new HashMap<FileStatus, String>();
    Set<FileStatus> trashSet = new HashSet<FileStatus>();
    Table<String, String, String> checkpointPaths = HashBasedTable.create();
    TestLocalStreamService service = new TestLocalStreamService(parser.getConfig(),
        cluster, null, new FSCheckpointProvider(checkpointDir),
        newStreamToProcess);
    service.createListing(localFs, localFs.getFileStatus(cluster.getDataDir
        ()), results, trashSet, checkpointPaths);
    Assert.assertEquals(getMinute(firstDate.getTime() + 60000),
        service.getLastProcessedMap().get(stream1));
  }

  @Test
  public void testMultipleCollectorsAllAtleastOneNonEmptyFiles() throws Exception {
    int numFiles = 6;
    Calendar calendar = Calendar.getInstance();
    calendar.add(Calendar.MINUTE, -7);
    Date firstDate = calendar.getTime();

    String path1 = cluster.getDataDir() + File.separator + stream1 +
        File.separator + cluster.getName() + File.separator;
    localFs.mkdirs(new Path(path1));
    for (int i = 0; i < numFiles - 2; i++) {
      calendar.add(Calendar.MINUTE, 1);
      String filenameStr = stream1 + "-" + TestLocalStreamService
          .getDateAsYYYYMMDDHHmm(calendar.getTime()) + "_" +
          TestLocalStreamService.idFormat.format(0);
      LOG.debug("Creating Test Data with filename [" + filenameStr + "]");
      FSDataOutputStream streamout = localFs.create(new Path(path1,
          filenameStr));
      streamout.close();
      Thread.sleep(1000);
    }
    for (int i = 0; i < 2; i++) {
      calendar.add(Calendar.MINUTE, 1);
      String filenameStr = stream1 + "-" + TestLocalStreamService
          .getDateAsYYYYMMDDHHmm(calendar.getTime()) + "_" +
          TestLocalStreamService.idFormat.format(0);
      LOG.debug("Creating Test Data with filename [" + filenameStr + "]");
      FSDataOutputStream streamout = localFs.create(new Path(path1,
          filenameStr));
      String content = "Creating Test data for teststream";
      Message msg = new Message(content.getBytes());
      long currentTimestamp = new Date().getTime();
      AuditUtil.attachHeaders(msg, currentTimestamp);
      byte[] encodeMsg = Base64.encodeBase64(msg.getData().array());
      streamout.write(encodeMsg);
      streamout.close();
      Thread.sleep(1000);
    }

    String path2 = cluster.getDataDir() + File.separator + stream1 +
        File.separator + "testcollector" + File.separator;
    localFs.mkdirs(new Path(path2));
    calendar.setTime(firstDate);
    for (int i = 0; i < 2; i++) {
      calendar.add(Calendar.MINUTE, 1);
      String filenameStr = stream1 + "-" + TestLocalStreamService
          .getDateAsYYYYMMDDHHmm(calendar.getTime()) + "_" +
          TestLocalStreamService.idFormat.format(0);
      LOG.debug("Creating Test Data with filename [" + filenameStr + "]");
      FSDataOutputStream streamout = localFs.create(new Path(path2,
          filenameStr));
      String content = "Creating Test data for teststream";
      Message msg = new Message(content.getBytes());
      long currentTimestamp = new Date().getTime();
      AuditUtil.attachHeaders(msg, currentTimestamp);
      byte[] encodeMsg = Base64.encodeBase64(msg.getData().array());
      streamout.write(encodeMsg);
      streamout.close();
      Thread.sleep(1000);
    }
    for (int i = 0; i < numFiles - 2; i++) {
      calendar.add(Calendar.MINUTE, 1);
      String filenameStr = stream1 + "-" + TestLocalStreamService
          .getDateAsYYYYMMDDHHmm(calendar.getTime()) + "_" +
          TestLocalStreamService.idFormat.format(0);
      LOG.debug("Creating Test Data with filename [" + filenameStr + "]");
      FSDataOutputStream streamout = localFs.create(new Path(path2,
          filenameStr));
      streamout.close();
      Thread.sleep(1000);
    }

    Set<String> newStreamToProcess = new HashSet<String>();
    newStreamToProcess.add(stream1);

    Map<FileStatus, String> results = new HashMap<FileStatus, String>();
    Set<FileStatus> trashSet = new HashSet<FileStatus>();
    Table<String, String, String> checkpointPaths = HashBasedTable.create();
    TestLocalStreamService service = new TestLocalStreamService(parser.getConfig(),
        cluster, null, new FSCheckpointProvider(checkpointDir),
        newStreamToProcess);
    service.createListing(localFs, localFs.getFileStatus(cluster.getDataDir
        ()), results, trashSet, checkpointPaths);
    Assert.assertEquals(getMinute(firstDate.getTime() + 60000),
        service.getLastProcessedMap().get(stream1));
  }
}
