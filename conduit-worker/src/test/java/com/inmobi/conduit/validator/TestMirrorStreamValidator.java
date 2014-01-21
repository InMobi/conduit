package com.inmobi.conduit.validator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.inmobi.conduit.Cluster;
import com.inmobi.conduit.ConduitConfig;
import com.inmobi.conduit.SourceStream;
import com.inmobi.conduit.utils.CalendarHelper;
import com.inmobi.conduit.utils.FileUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestMirrorStreamValidator extends AbstractTestStreamValidator {

  private static final Log LOG = LogFactory.getLog(TestMirrorStreamValidator.class);

  List<Path> holesInMerge = new ArrayList<Path>();
  List<Path> holesInMirror = new ArrayList<Path>();

  public TestMirrorStreamValidator() {
  }

  private void createMergeData(ConduitConfig config, Date date)
      throws IOException {
    Map<String, Cluster> primaryClusters = new HashMap<String, Cluster>();
    for (SourceStream stream : config.getSourceStreams().values()) {
      primaryClusters.put(stream.getName(),
          config.getPrimaryClusterForDestinationStream(stream.getName()));
    }
    for (String stream : primaryClusters.keySet()) {
      Cluster primaryCluster = primaryClusters.get(stream);
      FileSystem fs = null;
      Path streamLevelDir = null;
      Date nextDate = null;
      for (String cluster : config.getSourceStreams().get(stream)
          .getSourceClusters()) {
        fs = FileSystem.getLocal(new Configuration());
        streamLevelDir = new Path(primaryCluster.getFinalDestDirRoot()
            + stream);
        createData(fs, streamLevelDir, date, stream, cluster,5, 1, false);
        nextDate = CalendarHelper.addAMinute(date);
        createData(fs, streamLevelDir, nextDate, stream, cluster, 5, 1, false);
      }
      holesInMerge.addAll(createHoles(fs, streamLevelDir, nextDate));
    }
  }

  private List<Path> createHoles(FileSystem fs, Path streamLevelDir,
      Date nextDate)
          throws IOException {
    List<Path> holes = new ArrayList<Path>();
    // create two holes and a dummy directory in the end
    Date lastDate = CalendarHelper.addAMinute(nextDate);;
    for (int i = 0; i < 2; i++) {
      holes.add(CalendarHelper.getPathFromDate(lastDate, streamLevelDir));
      lastDate = CalendarHelper.addAMinute(lastDate);
    }
    fs.mkdirs(CalendarHelper.getPathFromDate(lastDate, streamLevelDir));
    return holes;
  }

  private void createMirrorData(ConduitConfig config,
      String streamName, Cluster mirrorCluster, Date date) throws IOException {
    Set<String> sourceClusters = config.getSourceStreams().get(streamName).
        getSourceClusters();
    Path streamLevelDir = null;
    FileSystem fs = null;
    Date nextDate = null;
    for (String srcCluster : sourceClusters) {
      fs = FileSystem.getLocal(new Configuration());
      streamLevelDir = new Path(mirrorCluster.getFinalDestDirRoot()
          + streamName);
      createData(fs, streamLevelDir, date, streamName, srcCluster, 5, 2, false);
      nextDate = CalendarHelper.addAMinute(date);
      createData(fs, streamLevelDir, nextDate, streamName, srcCluster, 5, 2, false);
    }
    holesInMirror.addAll(createHoles(fs, streamLevelDir, nextDate));
  }

  @Test
  public void testMirrorStreamValidator() throws Exception {
    Date date = new Date();
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    cal.add(Calendar.MINUTE, 1);
    Date nextDate = cal.getTime();
    cal.add(Calendar.MINUTE, 4);
    Date stopDate = cal.getTime();
    ConduitConfig config = setup("test-mirror-validator-conduit.xml");
    // clean up all root dir before generating test data
    cleanUp(config);
    createMergeData(config, date);
    Set<String> streamsSet = config.getSourceStreams().keySet();
    FileSystem fs = FileSystem.getLocal(new Configuration());
    String auditSrcJar = FileUtil.findContainingJar(
        com.inmobi.messaging.util.AuditUtil.class);
    for (String streamName : streamsSet) {
      for (Cluster cluster : config.getClusters().values()) {
        if (cluster.getMirroredStreams().contains(streamName)) {
          jarsPath = new Path(cluster.getTmpPath(), "jars");
          auditUtilJarDestPath = new Path(jarsPath, "messaging-client-core.jar");
          // Copy AuditUtil src jar to FS
          fs.copyFromLocalFile(new Path(auditSrcJar), auditUtilJarDestPath);
          createMirrorData(config, streamName, cluster, date);
          //check whether given start time is valid
          testStartTimeBeyondRetention(config,streamName, cluster.getName(),date,
              nextDate);
          // it tests missing paths for given a specific period
          testMirrorValidatorVerify(config,streamName, cluster.getName(),date,
              nextDate, false, false);
          // verify : it tests what all are the missing paths
          testMirrorValidatorVerify(config,streamName, cluster.getName(),date,
              stopDate, false, true);
          // fix : throw an exception as there are holes in source
          testMirrorValidatorFixWithHolesInSource(config,streamName,
              cluster.getName(), date, stopDate);
          // fix : It copies all the missing paths to mirror cluster
          testMirrorValidatorFix(config,streamName, cluster.getName(), date,
              stopDate);
          // reverify : should not contain any missing paths after fixing
          testMirrorValidatorVerify(config,streamName, cluster.getName(),date,
              stopDate, true, true);
        }
      }
    }
    cleanUp(config);
  }

  private void testStartTimeBeyondRetention(ConduitConfig config,
      String streamName, String mirrorClusterName, Date startTime,
      Date stopTime)
          throws Exception {
    Calendar cal = Calendar.getInstance();
    cal.setTime(startTime);
    cal.add(Calendar.HOUR_OF_DAY, -50);
    MirrorStreamValidator mirrorStreamValidator =
        new MirrorStreamValidator(config, streamName,
            mirrorClusterName, true, cal.getTime(), stopTime, 10);
    Throwable th = null;
    try {
      mirrorStreamValidator.execute();
    } catch (Exception e) {
      th = e;
      e.printStackTrace();
    }
    Assert.assertTrue(th instanceof IllegalArgumentException);
  }

  private void testMirrorValidatorVerify(ConduitConfig config,
      String streamName, String mirrorClusterName, Date startTime,
      Date stopTime, boolean reverify, boolean listedAllFiles)
          throws Exception {
    MirrorStreamValidator mirrorStreamValidator = new MirrorStreamValidator(
        config, streamName, mirrorClusterName, false, startTime, stopTime, 10);
    mirrorStreamValidator.execute();
    if (reverify) {
      Assert.assertEquals(mirrorStreamValidator.getMissingPaths().size(), 0);
    } else {
      if (listedAllFiles) {
        Assert.assertEquals(mirrorStreamValidator.getMissingPaths().size(),
            missingPaths.size());
        Assert.assertEquals(mirrorStreamValidator.getHolesInMerge(), holesInMerge);
        Assert.assertEquals(mirrorStreamValidator.getHolesInMirror(), holesInMirror);
      } else {
        Assert.assertEquals(mirrorStreamValidator.getMissingPaths().size(),
            missingPaths.size()/2);
      }
    }
  }

  private void testMirrorValidatorFixWithHolesInSource(ConduitConfig config,
      String streamName, String mirrorClusterName, Date startTime, Date stopTime)
          throws Exception {
    MirrorStreamValidator mirrorStreamValidator = new MirrorStreamValidator(
        config, streamName, mirrorClusterName, true, startTime, stopTime, 10);
    Throwable th = null;
    try {
      mirrorStreamValidator.execute();
    } catch (Exception e) {
      th = e;
    }
    Assert.assertTrue(th instanceof IllegalStateException);
  }
  
  private void testMirrorValidatorFix(ConduitConfig config,
      String streamName, String mirrorClusterName, Date startTime, Date stopTime)
          throws Exception {
    MirrorStreamValidator mirrorStreamValidator = new MirrorStreamValidator(
        config, streamName, mirrorClusterName, true, startTime, stopTime, 10);
    FileSystem fs = FileSystem.getLocal(new Configuration());
    for (Path path : holesInMerge) {
      fs.mkdirs(path);
    }
    mirrorStreamValidator.execute();
    Assert.assertEquals(mirrorStreamValidator.getMissingPaths().size(),
        missingPaths.size() + holesInMerge.size());
  }
}
