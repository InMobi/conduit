package com.inmobi.conduit;

import java.io.File;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.inmobi.conduit.metrics.ConduitMetrics;
import com.inmobi.conduit.metrics.SlidingTimeWindowGauge;
import com.inmobi.conduit.local.TestLocalStreamService;

public class PublishMissingPathsTest {

  private static Logger LOG = Logger.getLogger(PublishMissingPathsTest.class);

  public static void VerifyMissingPublishPaths(FileSystem fs, long timeInMillis,
                                         long behinddate,
                                         String basepublishPaths,
                                         long timeDiff) throws Exception {
    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(behinddate);
    VerifyMissingPublishPaths(fs, timeInMillis, cal, basepublishPaths, timeDiff);
  }

  public static void VerifyMissingPublishPaths(FileSystem fs,
                                               long todaysdate,
                                               Calendar behinddate,
                                               String basepublishPaths,
                                               long timeDiff) throws  Exception {
    long diff = todaysdate - behinddate.getTimeInMillis();
    while (diff > timeDiff) {
      String checkcommitpath = basepublishPaths + File.separator
          + Cluster.getDateAsYYYYMMDDHHMNPath(behinddate.getTime());
      LOG.debug("Checking for Created Missing Path: " + checkcommitpath);
      Assert.assertTrue(fs.exists(new Path(checkcommitpath)));
      behinddate.add(Calendar.MINUTE, 1);
      diff = todaysdate - behinddate.getTimeInMillis();
    }
  }

  @BeforeMethod
  public void beforeTest() throws Exception{
    AbstractService.clearHCatInMemoryMaps();
    Conduit.setHCatEnabled(false);
    Properties prop = new Properties();
    prop.setProperty("com.inmobi.conduit.metrics.enabled", "true");
    ConduitMetrics.init(prop);
    ConduitMetrics.startAll();
  }

  @AfterMethod
  public void afterTest() throws Exception{
    ConduitMetrics.stopAll();;
  }

  @Test
  public void testPublishMissingPaths() throws Exception {
    ConduitConfigParser configParser = new ConduitConfigParser(
        "test-lss-pub-conduit.xml");
    Set<String> streamsToProcess = new HashSet<String>();
    ConduitConfig config = configParser.getConfig();
    streamsToProcess.addAll(config.getSourceStreams().keySet());
    FileSystem fs = FileSystem.getLocal(new Configuration());

    ArrayList<Cluster> clusterList = new ArrayList<Cluster>(config
        .getClusters().values());
    Cluster cluster = clusterList.get(0);
    fs.delete(new Path(cluster.getRootDir()), true);
    TestLocalStreamService service = new TestLocalStreamService(config,
        cluster, null, new FSCheckpointProvider(cluster.getCheckpointDir()),
        streamsToProcess);

    ArrayList<SourceStream> sstreamList = new ArrayList<SourceStream>(config
        .getSourceStreams().values());
    SourceStream sstream1 = sstreamList.get(0);
    SourceStream sstream2 = sstreamList.get(1);


    String basepublishPaths = cluster.getLocalFinalDestDirRoot()
        + sstream1.getName() + File.separator;
    fs.mkdirs(new Path(basepublishPaths));
    // no empty directories are present for the stream
    {
      Set<String> streamsToProcessThisRun = new HashSet<String>();
      streamsToProcessThisRun.add(sstream1.getName());
      long firstCommitTime = cluster.getCommitTime();
      service.publishMissingPaths(fs, cluster.getLocalFinalDestDirRoot(),
          firstCommitTime, streamsToProcessThisRun);
      assert fs.listStatus(new Path(basepublishPaths)).length == 0;
      Thread.sleep(60000);
      Calendar todaysdate = new GregorianCalendar();
      long secondCommitTime = cluster.getCommitTime();
      service.publishMissingPaths(fs, cluster.getLocalFinalDestDirRoot(),
          secondCommitTime, streamsToProcessThisRun);
      assert fs.listStatus(new Path(basepublishPaths)).length != 0;
      VerifyMissingPublishPaths(fs, todaysdate.getTimeInMillis(),
          firstCommitTime, basepublishPaths, 60000);
    }

    // get previous run time by calculating date from last directory present
    // for the stream
    Calendar behinddate = new GregorianCalendar();

    behinddate.add(Calendar.HOUR_OF_DAY, -2);
    behinddate.set(Calendar.SECOND, 0);

    basepublishPaths = cluster.getLocalFinalDestDirRoot()
        + sstream2.getName() + File.separator;
    String publishPaths = basepublishPaths
        + Cluster.getDateAsYYYYMMDDHHMNPath(behinddate.getTime());
    Set<String> streamsToProcessThisRun = new HashSet<String>();
    streamsToProcessThisRun.add(sstream2.getName());

    fs.mkdirs(new Path(publishPaths));
    {
      Calendar todaysdate = new GregorianCalendar();
      long commitTime = cluster.getCommitTime();      
      service.publishMissingPaths(fs, cluster.getLocalFinalDestDirRoot(),
          commitTime, streamsToProcessThisRun);
      VerifyMissingPublishPaths(fs, todaysdate.getTimeInMillis(), behinddate,
          basepublishPaths, 180000);
    }

    {
      Calendar todaysdate = new GregorianCalendar();
      long commitTime = cluster.getCommitTime();
      service.publishMissingPaths(fs, cluster.getLocalFinalDestDirRoot(),
          commitTime, streamsToProcessThisRun);
      VerifyMissingPublishPaths(fs, todaysdate.getTimeInMillis(), behinddate,
          basepublishPaths, 180000);
    }

    fs.delete(new Path(cluster.getRootDir()), true);

    fs.close();
    Assert.assertTrue(ConduitMetrics.<SlidingTimeWindowGauge>getMetric
        ("LocalStreamService","emptyDir.create","test1").getValue() >0 );
    Assert.assertTrue(ConduitMetrics.<SlidingTimeWindowGauge>getMetric
        ("LocalStreamService","emptyDir.create","test2").getValue() >0 );
  }
}
