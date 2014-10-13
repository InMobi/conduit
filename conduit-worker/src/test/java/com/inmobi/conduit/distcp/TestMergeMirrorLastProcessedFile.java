package com.inmobi.conduit.distcp;

import com.inmobi.conduit.Cluster;
import com.inmobi.conduit.ConduitConfigParser;
import com.inmobi.conduit.metrics.ConduitMetrics;
import com.inmobi.conduit.utils.CalendarHelper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Properties;

public class TestMergeMirrorLastProcessedFile {
  private static Logger LOG = Logger.getLogger(TestMergeMirrorLastProcessedFile.class);

  private static FileSystem localFs;
  private static final String checkpointDir =
      "/tmp/test-conduit/conduit-distcps-checkpoint";
  private static Cluster cluster1, cluster2;
  private static ConduitConfigParser parser;
  private static String stream1, stream2;

  @BeforeTest
  public void setup() throws Exception {
    localFs = FileSystem.getLocal(new Configuration());
    parser = new ConduitConfigParser("src/test/resources/test-mss-conduit2.xml");
    cluster1 = parser.getConfig().getClusters().get("testcluster1");
    cluster2 = parser.getConfig().getClusters().get("testcluster2");
    Iterator<String> setIterator = cluster1.getSourceStreams().iterator();
    if (setIterator.hasNext()) {
      stream1 = setIterator.next();
    }
    if (setIterator.hasNext()) {
      stream2 = setIterator.next();
    }
  }

  @AfterTest
  public void cleanup() throws Exception {
    if (localFs.exists(new Path(cluster1.getLocalFinalDestDirRoot()))) {
      localFs.delete(new Path(cluster1.getLocalFinalDestDirRoot()), true);
    }
    if (localFs.exists(new Path(cluster1.getFinalDestDirRoot()))) {
      localFs.delete(new Path(cluster1.getFinalDestDirRoot()), true);
    }
    if (localFs.exists(new Path(cluster2.getFinalDestDirRoot()))) {
      localFs.delete(new Path(cluster2.getFinalDestDirRoot()), true);
    }
    if (localFs.exists(new Path(checkpointDir))) {
      localFs.delete(new Path(checkpointDir), true);
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

  private Long getMinute(Long time) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTimeInMillis(time);
    calendar.set(Calendar.MILLISECOND, 0);
    calendar.set(Calendar.SECOND, 0);
    return calendar.getTimeInMillis();
  }

  private boolean createFile(FileSystem fs, Path path, Cluster cluster,
                          String stream, Calendar cal) {
    String filenameStr1 = cluster.getName() + "-" + stream + "-" +
      CalendarHelper.getDateTimeAsString(cal) + "_" + MergeCheckpointTest
      .idFormat.format(0);
    Path file1 = new Path(path, filenameStr1 + ".gz");
    Compressor gzipCompressor = null;
    try {
      fs.mkdirs(path);
      GzipCodec gzipCodec = ReflectionUtils.newInstance(GzipCodec.class,
          new Configuration());
      gzipCompressor = CodecPool.getCompressor(gzipCodec);
      FSDataOutputStream out = fs.create(file1);
      out.writeChars("Test message");
      OutputStream compressedOut = gzipCodec.createOutputStream(out,
          gzipCompressor);
      compressedOut.close();
    } catch (IOException e) {
      LOG.error(e);
      return false;
    } finally {
      if (gzipCompressor != null)
        CodecPool.returnCompressor(gzipCompressor);
    }
    return true;
  }

  @Test
  public void testMergeEmptyDirectories() throws Exception {
    int numberOfFiles = 9;
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.MINUTE, -10);
    for(int i = 0; i < numberOfFiles; i++) {
      cal.add(Calendar.MINUTE, 1);
      localFs.mkdirs(new Path(cluster1.getLocalDestDir(stream1,
          cal.getTime())));
      localFs.mkdirs(new Path(cluster1.getLocalDestDir(stream2,
          cal.getTime())));
    }

    TestMergedStreamService service = new TestMergedStreamService(parser
        .getConfig(), cluster1, cluster2, null, cluster1.getSourceStreams());
    service.getDistCPInputFile();
    Assert.assertEquals(getMinute(cal.getTimeInMillis() - 60000),
        service.getLastProcessedMap().get(stream1));
    Assert.assertEquals(getMinute(cal.getTimeInMillis() - 60000),
        service.getLastProcessedMap().get(stream2));
  }

  @Test
  public void testMergeNonEmptyDirectoriesAtEnd() throws Exception {
    int numberOfFiles = 9;
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.MINUTE, -10);
    for(int i = 0; i < numberOfFiles - 2; i++) {
      cal.add(Calendar.MINUTE, 1);
      localFs.mkdirs(new Path(cluster1.getLocalDestDir(stream1,
          cal.getTime())));
      localFs.mkdirs(new Path(cluster1.getLocalDestDir(stream2,
          cal.getTime())));
    }
    for(int i = 0; i < 2; i++) {
      cal.add(Calendar.MINUTE, 1);
      createFile(localFs, new Path(cluster1.getLocalDestDir(stream1,
          cal.getTime())), cluster1, stream1, cal);
      createFile(localFs, new Path(cluster1.getLocalDestDir(stream2,
          cal.getTime())), cluster1, stream2, cal);
    }

    TestMergedStreamService service = new TestMergedStreamService(parser
        .getConfig(), cluster1, cluster2, null, cluster1.getSourceStreams());
    service.getDistCPInputFile();
    Assert.assertEquals(getMinute(cal.getTimeInMillis() - 60000),
        service.getLastProcessedMap().get(stream1));
    Assert.assertEquals(getMinute(cal.getTimeInMillis() - 60000),
        service.getLastProcessedMap().get(stream2));
  }

  @Test
  public void testMergeEmptyDirectoriesAtEnd() throws Exception {
    int numberOfFiles = 9;
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.MINUTE, -10);
    for(int i = 0; i < 2; i++) {
      cal.add(Calendar.MINUTE, 1);
      createFile(localFs, new Path(cluster1.getLocalDestDir(stream1,
          cal.getTime())), cluster1, stream1, cal);
      createFile(localFs, new Path(cluster1.getLocalDestDir(stream2,
          cal.getTime())), cluster1, stream2, cal);
    }
    for(int i = 0; i < numberOfFiles - 2; i++) {
      cal.add(Calendar.MINUTE, 1);
      localFs.mkdirs(new Path(cluster1.getLocalDestDir(stream1,
          cal.getTime())));
      localFs.mkdirs(new Path(cluster1.getLocalDestDir(stream2,
          cal.getTime())));
    }

    TestMergedStreamService service = new TestMergedStreamService(parser
        .getConfig(), cluster1, cluster2, null, cluster1.getSourceStreams());
    service.getDistCPInputFile();
    Assert.assertEquals(getMinute(cal.getTimeInMillis() - 60000),
        service.getLastProcessedMap().get(stream1));
    Assert.assertEquals(getMinute(cal.getTimeInMillis() - 60000),
        service.getLastProcessedMap().get(stream2));
  }

  @Test
  public void testMirrorEmptyDirectories() throws Exception {
    int numberOfFiles = 9;
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.MINUTE, -10);
    for(int i = 0; i < numberOfFiles; i++) {
      cal.add(Calendar.MINUTE, 1);
      localFs.mkdirs(new Path(cluster2.getFinalDestDir(stream1,
          cal.getTime().getTime())));
      localFs.mkdirs(new Path(cluster2.getFinalDestDir(stream2,
          cal.getTime().getTime())));
    }

    TestMirrorStreamService service = new TestMirrorStreamService(parser
        .getConfig(), cluster2, cluster1, null, cluster1.getSourceStreams());
    service.getDistCPInputFile();
    Assert.assertEquals(getMinute(cal.getTimeInMillis() - 60000),
        service.getLastProcessedMap().get(stream1));
    Assert.assertEquals(getMinute(cal.getTimeInMillis() - 60000),
        service.getLastProcessedMap().get(stream2));
  }

  @Test
  public void testMirrorNonEmptyDirectoriesAtEnd() throws Exception {
    int numberOfFiles = 9;
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.MINUTE, -10);
    for(int i = 0; i < numberOfFiles - 2; i++) {
      cal.add(Calendar.MINUTE, 1);
      localFs.mkdirs(new Path(cluster2.getFinalDestDir(stream1,
          cal.getTime().getTime())));
      localFs.mkdirs(new Path(cluster2.getFinalDestDir(stream2,
          cal.getTime().getTime())));
    }
    for(int i = 0; i < 2; i++) {
      cal.add(Calendar.MINUTE, 1);
      createFile(localFs, new Path(cluster2.getFinalDestDir(stream1,
          cal.getTime().getTime())), cluster2, stream1, cal);
      createFile(localFs, new Path(cluster2.getFinalDestDir(stream2,
          cal.getTime().getTime())), cluster2, stream2, cal);
    }

    TestMirrorStreamService service = new TestMirrorStreamService(parser
        .getConfig(), cluster2, cluster1, null, cluster1.getSourceStreams());
    service.getDistCPInputFile();
    Assert.assertEquals(getMinute(cal.getTimeInMillis() - 60000),
        service.getLastProcessedMap().get(stream1));
    Assert.assertEquals(getMinute(cal.getTimeInMillis() - 60000),
        service.getLastProcessedMap().get(stream2));
  }

  @Test
  public void testMirrorEmptyDirectoriesAtEnd() throws Exception {
    int numberOfFiles = 9;
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.MINUTE, -10);
    for(int i = 0; i < 2; i++) {
      cal.add(Calendar.MINUTE, 1);
      createFile(localFs, new Path(cluster2.getFinalDestDir(stream1,
          cal.getTime().getTime())), cluster2, stream1, cal);
      createFile(localFs, new Path(cluster2.getFinalDestDir(stream2,
          cal.getTime().getTime())), cluster2, stream2, cal);
    }
    for(int i = 0; i < numberOfFiles - 2; i++) {
      cal.add(Calendar.MINUTE, 1);
      localFs.mkdirs(new Path(cluster2.getFinalDestDir(stream1,
          cal.getTime().getTime())));
      localFs.mkdirs(new Path(cluster2.getFinalDestDir(stream2,
          cal.getTime().getTime())));
    }

    TestMirrorStreamService service = new TestMirrorStreamService(parser
        .getConfig(), cluster2, cluster1, null, cluster1.getSourceStreams());
    service.getDistCPInputFile();
    Assert.assertEquals(getMinute(cal.getTimeInMillis() - 60000),
        service.getLastProcessedMap().get(stream1));
    Assert.assertEquals(getMinute(cal.getTimeInMillis() - 60000),
        service.getLastProcessedMap().get(stream2));
  }
}
