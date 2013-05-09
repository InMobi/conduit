package com.inmobi.databus.validator;

import java.io.IOException;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import com.inmobi.databus.DatabusConfigParser;
import com.inmobi.databus.SourceStream;
import com.inmobi.databus.utils.CalendarHelper;

public class TestMirrorStreamValidator extends AbstractTestStreamValidator {

  private static final Log LOG = LogFactory.getLog(TestMirrorStreamValidator.class);

  public TestMirrorStreamValidator() {
  }

  private void createMergeData(DatabusConfig config, Date date)
      throws IOException {
    Map<String, Cluster> primaryClusters = new HashMap<String, Cluster>();
    for (SourceStream stream : config.getSourceStreams().values()) {
      primaryClusters.put(stream.getName(),
          config.getPrimaryClusterForDestinationStream(stream.getName()));
    }
    for (String stream : primaryClusters.keySet()) {
      Cluster primaryCluster = primaryClusters.get(stream);
      for (String cluster : config.getSourceStreams().get(stream)
          .getSourceClusters()) {
        FileSystem fs = FileSystem.getLocal(new Configuration());
        Path streamLevelDir = new Path(primaryCluster.getFinalDestDirRoot()
            + stream);
        createData(fs, streamLevelDir, date, stream, cluster,5, 1);
        Date nextDate = CalendarHelper.addAMinute(date);
        createData(fs, streamLevelDir, nextDate, stream, cluster, 5, 1);
        // Add a dummy empty directory in the end
        Date lastDate = CalendarHelper.addAMinute(nextDate);
        fs.mkdirs(CalendarHelper.getPathFromDate(lastDate, streamLevelDir));
      }
    }
  }

  private void createMirrorData(DatabusConfig config,
      String streamName, Cluster mirrorCluster, Date date) throws IOException {
    Set<String> sourceClusters = config.getSourceStreams().get(streamName).
        getSourceClusters();
    for (String srcCluster : sourceClusters) {
      FileSystem fs = FileSystem.getLocal(new Configuration());
      Path streamLevelDir = new Path(mirrorCluster.getFinalDestDirRoot()
          + streamName);
      createData(fs, streamLevelDir, date, streamName, srcCluster, 5, 2);
      Date nextDate = CalendarHelper.addAMinute(date);
      createData(fs, streamLevelDir, nextDate, streamName, srcCluster, 5, 2);
    }
  }

  @Test
  public void testMergeStreamValidator() throws Exception {
    Date date = new Date();
    Date nextDate = CalendarHelper.addAMinute(date);
    Date stopDate = CalendarHelper.addAMinute(nextDate);
    DatabusConfig config = setup("test-mirror-validator-databus.xml");
    // clean up all root dir before generating test data
    cleanUp(config);
    createMergeData(config, date);
    Set<String> streamsSet = config.getSourceStreams().keySet();
    for (String streamName : streamsSet) {
      for (Cluster cluster : config.getClusters().values()) {
        if (cluster.getMirroredStreams().contains(streamName)) {
          createMirrorData(config, streamName, cluster, date);
          // verify : it tests what all are the missing paths
          testMirrorValidatorVerify(config,streamName, cluster.getName(),date,
              stopDate, false);
          // fix : It copies all the missing paths to mirror cluster
          testMirrorValidatorFix(config,streamName, cluster.getName(), date,
              stopDate);
          // reverify : should not contain any missing paths after fixing
          testMirrorValidatorVerify(config,streamName, cluster.getName(),date,
              stopDate, true);
        }
      }
    }
    cleanUp(config);
  }

  private void testMirrorValidatorVerify(DatabusConfig config,
      String streamName, String mirrorClusterName, Date startTime,
      Date stopTime, boolean reverify)
          throws Exception {  
    MirrorStreamValidator mirrorStreamValidator = new MirrorStreamValidator(
        config, streamName, mirrorClusterName, false, startTime, stopTime, 10);
    mirrorStreamValidator.execute();
    if (reverify) {
      Assert.assertEquals(mirrorStreamValidator.getMissingPaths().size(), 0);
    } else {
      Assert.assertEquals(mirrorStreamValidator.getMissingPaths().size(),
          missingPaths.size());
    }
  }

  private void testMirrorValidatorFix(DatabusConfig config,
      String streamName, String mirrorClusterName, Date startTime, Date stopTime)
          throws Exception {
    MirrorStreamValidator mirrorStreamValidator = new MirrorStreamValidator(
        config, streamName, mirrorClusterName, true, startTime, stopTime, 10);
    mirrorStreamValidator.execute();
    Assert.assertEquals(mirrorStreamValidator.getMissingPaths().size(),
        missingPaths.size());
  }
}
