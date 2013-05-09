package com.inmobi.databus.validator;

import java.io.IOException;
import java.util.Date;
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

public class TestMergeStreamValidator extends AbstractTestStreamValidator {
  private static final Log LOG = LogFactory.getLog(TestMergeStreamValidator.class);

  private void createLocalData(DatabusConfig config,
      Date date, Cluster cluster, String stream) throws IOException {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    Path streamLevelDir = new Path(cluster.getLocalFinalDestDirRoot()
        + stream);
    createData(fs, streamLevelDir, date, stream, cluster.getName(), 5 , 1);
    Date nextDate = CalendarHelper.addAMinute(date);
    createData(fs, streamLevelDir, nextDate, stream, cluster.getName(), 5, 1);
    // Add a dummy empty directory in the end
    Date lastDate = CalendarHelper.addAMinute(nextDate);
    fs.mkdirs(CalendarHelper.getPathFromDate(lastDate, streamLevelDir));
  }

  private void createMergeData(DatabusConfig config, Date date,
      Cluster primaryCluster, String stream)
          throws IOException {
    for (String cluster : config.getSourceStreams().get(stream)
        .getSourceClusters()) {
      FileSystem fs = FileSystem.getLocal(new Configuration());
      Path streamLevelDir = new Path(primaryCluster.getFinalDestDirRoot()
          + stream);
      createData(fs, streamLevelDir, date, stream, cluster, 5, 2);
      Date nextDate = CalendarHelper.addAMinute(date);
      createData(fs, streamLevelDir, nextDate, stream, cluster, 5, 2);
      // Add a dummy empty directory in the end
      Date lastDate = CalendarHelper.addAMinute(nextDate);
      fs.mkdirs(CalendarHelper.getPathFromDate(lastDate, streamLevelDir));
    }
  }

  @Test
  public void testMergeStreamValidator() throws Exception {
    Date date = new Date();
    Date nextDate = CalendarHelper.addAMinute(date);
    Date stopDate = CalendarHelper.addAMinute(nextDate);
    DatabusConfig config = setup("test-merge-validator-databus.xml");
    Set<String> streamsSet = config.getSourceStreams().keySet();
    for (String streamName : streamsSet) {
      Cluster mergedCluster = null;
      for (Cluster cluster : config.getClusters().values()) {
        if (cluster.getSourceStreams().contains(streamName)) {
          createLocalData(config, date, cluster, streamName);
        }
        if (cluster.getPrimaryDestinationStreams().contains(streamName)) {
          mergedCluster = cluster;
          createMergeData(config, date, cluster, streamName);
        }
      }
      if (mergedCluster != null) {
        testMergeStreamValidatorVerify(date, stopDate, config, streamName,
            mergedCluster, false);
        testMergeValidatorFix(date, stopDate, config, streamName, mergedCluster); 
      }
    }
  }

  private void testMergeValidatorFix(Date date, Date stopDate,
      DatabusConfig config, String streamName, Cluster mergedCluster)
          throws Exception {
    MergedStreamValidator mergeStreamValidator;
    mergeStreamValidator =
        new MergedStreamValidator(config, streamName,
            mergedCluster.getName(), true, date, stopDate, 10);
    mergeStreamValidator.execute();
  }

  private void testMergeStreamValidatorVerify(Date date, Date stopDate,
      DatabusConfig config, String streamName, Cluster mergeCluster,
      boolean reverify)
          throws Exception {
    MergedStreamValidator mergeStreamValidator =
        new MergedStreamValidator(config, streamName,
            mergeCluster.getName(), false, date, stopDate, 10);
    mergeStreamValidator.execute();
    if (reverify) {
      Assert.assertEquals(mergeStreamValidator.getMissingPaths().size(), 0);
    } else {
      Assert.assertEquals(mergeStreamValidator.getMissingPaths().size(),
          missingPaths.size());
    }
  }
}