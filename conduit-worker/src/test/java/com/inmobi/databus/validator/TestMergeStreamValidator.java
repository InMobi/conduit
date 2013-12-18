package com.inmobi.databus.validator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.utils.CalendarHelper;
import com.inmobi.databus.utils.FileUtil;

public class TestMergeStreamValidator extends AbstractTestStreamValidator {
  private static final Log LOG = LogFactory.getLog(TestMergeStreamValidator.class);
  List<Path> holesInLocal = new ArrayList<Path>();
  List<Path> holesInMerge = new ArrayList<Path>();

  private void createLocalData(DatabusConfig config,
      Date date, Cluster cluster, String stream) throws IOException {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    Path streamLevelDir = new Path(cluster.getLocalFinalDestDirRoot()
        + stream);
    createData(fs, streamLevelDir, date, stream, cluster.getName(), 5 , 1, false);
    Date nextDate = CalendarHelper.addAMinute(date);
    createData(fs, streamLevelDir, nextDate, stream, cluster.getName(), 5, 1, false);
    createHolesInLocal(fs, streamLevelDir, nextDate);
  }

  private void createHolesInLocal(FileSystem fs, Path streamLevelDir, Date nextDate)
      throws IOException {
    // create two holes and a dummy empty directory in the end
    Date lastDate = CalendarHelper.addAMinute(nextDate);
    for (int i = 0; i < 2; i++) {
      holesInLocal.add(CalendarHelper.getPathFromDate(lastDate, streamLevelDir));
      lastDate = CalendarHelper.addAMinute(lastDate);
    }
    fs.mkdirs(CalendarHelper.getPathFromDate(lastDate, streamLevelDir));
  }

  private void createMergeData(DatabusConfig config, Date date,
      Cluster primaryCluster, String stream)
          throws IOException {
    Path streamLevelDir = null;
    FileSystem fs = null;
    for (String cluster : config.getSourceStreams().get(stream)
        .getSourceClusters()) {
      fs = FileSystem.getLocal(new Configuration());
      streamLevelDir = new Path(primaryCluster.getFinalDestDirRoot()
          + stream);
      createData(fs, streamLevelDir, date, stream, cluster, 5, 2, true);
      Date nextDate = CalendarHelper.addAMinute(date);
      createData(fs, streamLevelDir, nextDate, stream, cluster, 5, 2, true);    
    }
    Date nextDate = CalendarHelper.addAMinute(date);
    createHolesInMerge(fs, streamLevelDir, nextDate);
  }

  private void createHolesInMerge(FileSystem fs, Path streamLevelDir, Date nextDate)
      throws IOException {
    // create a dummy empty directory in the end
    Date lastDate = CalendarHelper.addAMinute(nextDate);
    lastDate = CalendarHelper.addAMinute(lastDate);
    holesInMerge.add(CalendarHelper.getPathFromDate(lastDate, streamLevelDir));
    lastDate = CalendarHelper.addAMinute(lastDate);
    fs.mkdirs(CalendarHelper.getPathFromDate(lastDate, streamLevelDir));
  }

  @Test
  public void testMergeStreamValidator() throws Exception {
    Date date = new Date();
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    cal.add(Calendar.MINUTE, 1);
    Date nextDate = cal.getTime();
    cal.add(Calendar.MINUTE, 4);
    Date stopDate = cal.getTime();
    DatabusConfig config = setup("test-merge-validator-databus.xml");
    // clean up root dir before generating test data
    cleanUp(config);
    FileSystem fs = FileSystem.getLocal(new Configuration());
    String auditSrcJar = FileUtil.findContainingJar(
        com.inmobi.messaging.util.AuditUtil.class);
    Set<String> streamsSet = config.getSourceStreams().keySet();
    for (String streamName : streamsSet) {
      Cluster mergedCluster = null;
      for (Cluster cluster : config.getClusters().values()) {
        if (cluster.getSourceStreams().contains(streamName)) {
          createLocalData(config, date, cluster, streamName);
        }
        if (cluster.getPrimaryDestinationStreams().contains(streamName)) {
          mergedCluster = cluster;
          jarsPath = new Path(mergedCluster.getTmpPath(), "jars");
          auditUtilJarDestPath = new Path(jarsPath, "messaging-client-core.jar");
          // Copy AuditUtil src jar to FS
          fs.copyFromLocalFile(new Path(auditSrcJar), auditUtilJarDestPath);
          createMergeData(config, date, cluster, streamName);
        }
      }
      if (mergedCluster != null) {
        testStartDateBeyondRetention(date, stopDate, config, streamName,
            mergedCluster);

        testMergeStreamValidatorVerify(date, nextDate, config, streamName,
            mergedCluster, false, false);
        testMergeStreamValidatorVerify(date, stopDate, config, streamName,
            mergedCluster, false, true);
        testMergeValidatorFix(date, stopDate, config, streamName, mergedCluster);
        testMergeStreamValidatorVerify(date, stopDate, config, streamName,
            mergedCluster, true, true);
      }
    }
    cleanUp(config);
  }

  private void testStartDateBeyondRetention(Date date, Date stopDate,
      DatabusConfig config, String streamName, Cluster mergedCluster)
          throws Exception {
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    cal.add(Calendar.HOUR_OF_DAY, -50);
    MergedStreamValidator mergeStreamValidator =
        new MergedStreamValidator(config, streamName,
            mergedCluster.getName(), true, cal.getTime(), stopDate, 10);
    Throwable th = null;
    try {
      mergeStreamValidator.execute();
    } catch (Exception e) {
      th = e;
      e.printStackTrace();
    }
    Assert.assertTrue(th instanceof IllegalArgumentException);
  }

  private void testMergeValidatorFix(Date date, Date stopDate,
      DatabusConfig config, String streamName, Cluster mergedCluster)
          throws Exception {
    MergedStreamValidator mergeStreamValidator =
        new MergedStreamValidator(config, streamName,
            mergedCluster.getName(), true, date, stopDate, 10);
    mergeStreamValidator.execute();
  }

  private void testMergeStreamValidatorVerify(Date date, Date stopDate,
      DatabusConfig config, String streamName, Cluster mergeCluster,
      boolean reverify, boolean listedAllFiles)
          throws Exception {
    MergedStreamValidator mergeStreamValidator =
        new MergedStreamValidator(config, streamName,
            mergeCluster.getName(), false, date, stopDate, 10);
    mergeStreamValidator.execute();
    if (reverify) {
      Assert.assertEquals(mergeStreamValidator.getMissingPaths().size(), 0);
      Assert.assertEquals(mergeStreamValidator.getHolesInMerge().size(), 0);
    } else {
      if (listedAllFiles) {
        Assert.assertEquals(mergeStreamValidator.getMissingPaths().size(),
            missingPaths.size());
        Assert.assertEquals(holesInLocal, mergeStreamValidator.getHolesInLocal());
        Assert.assertEquals(holesInMerge, mergeStreamValidator.getHolesInMerge());
      } else {
        Assert.assertEquals(mergeStreamValidator.getMissingPaths().size(),
            missingPaths.size()/2);
      }
    }
    Assert.assertEquals(duplicateFiles.size(),
        mergeStreamValidator.getDuplicateFiles().size());
    Assert.assertTrue(duplicateFiles.containsAll(
        mergeStreamValidator.getDuplicateFiles()));
  }
}