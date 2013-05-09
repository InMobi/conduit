package com.inmobi.databus.validator;

import java.io.IOException;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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
import com.inmobi.databus.DatabusConfigParser;
import com.inmobi.databus.utils.CalendarHelper;

public class TestMergeStreamValidator extends AbstractTestStreamValidator {
  private static final Log LOG = LogFactory.getLog(TestMergeStreamValidator.class);
  private List<Path> missingPaths = new ArrayList<Path>();
  
  private static final NumberFormat idFormat = NumberFormat.getInstance();
  static {
    idFormat.setGroupingUsed(false);
    idFormat.setMinimumIntegerDigits(5);
  }

  protected static String getDateAsYYYYMMDDHHmm(Date date) {
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
    return dateFormat.format(date);
  }
  
  private DatabusConfig setup(String configFile) throws Exception {
    DatabusConfigParser configParser;
    DatabusConfig config = null;
    configParser = new DatabusConfigParser(configFile);
    config = configParser.getConfig();

    for (Cluster cluster : config.getClusters().values()) {
      FileSystem fs = FileSystem.get(cluster.getHadoopConf());
      fs.delete(new Path(cluster.getRootDir()), true);
    }
    return config;
  }

  private void createFiles(FileSystem fs, Date date, String streamName,
      String clusterName, Path path, int i) {
    String fileNameStr = new String(clusterName + "-" + streamName + "-" +
        getDateAsYYYYMMDDHHmm(date)+ "_" + idFormat.format(i));
    Path file = new Path(path, fileNameStr + ".gz");
    try {
      fs.create(file);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void createData(FileSystem fs, Path dir, Date date,
      String streamName, String clusterName, int numFiles, int incrementNumber) {
    Path path = CalendarHelper.getPathFromDate(date, dir);
    for (int i = 1; i <= numFiles; i = i + incrementNumber) {
      createFiles(fs, date, streamName, clusterName, path, i);
      if (incrementNumber != 1) {
        for (int j = i + 1; (j < i + incrementNumber) && (i != numFiles); j++) {
          // these are the missing paths
          String fileNameStr = new String(clusterName + "-" + streamName + "-" +
              getDateAsYYYYMMDDHHmm(date)+ "_" + idFormat.format(j));
          Path file = new Path(path, fileNameStr + ".gz");
          missingPaths.add(file);
        }
      }
    }
  }

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
      createData(fs, streamLevelDir, date, stream, cluster,
          5, 2);
      Date nextDate = CalendarHelper.addAMinute(date);
      createData(fs, streamLevelDir, nextDate, stream, cluster,
          5, 2);
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
    DatabusConfig config = setup("test-mirror-validator-databus.xml");
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