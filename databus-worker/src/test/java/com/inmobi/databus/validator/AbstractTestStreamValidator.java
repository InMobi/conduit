package com.inmobi.databus.validator;

import java.io.IOException;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.DatabusConfigParser;
import com.inmobi.databus.utils.CalendarHelper;

public class AbstractTestStreamValidator {

  protected static final NumberFormat idFormat = NumberFormat.getInstance();
  protected List<Path> missingPaths = new ArrayList<Path>();
  protected List<Path> duplicateFiles = new ArrayList<Path>();
  static {
    idFormat.setGroupingUsed(false);
    idFormat.setMinimumIntegerDigits(5);
  }

  protected static String getDateAsYYYYMMDDHHmm(Date date) {
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
    return dateFormat.format(date);
  }

  protected DatabusConfig setup(String configFile) throws Exception {
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

  protected Path createFiles(FileSystem fs, Date date, String streamName,
      String clusterName, Path path, int i) throws IOException {
    String fileNameStr = new String(clusterName + "-" + streamName + "-" +
        getDateAsYYYYMMDDHHmm(date)+ "_" + idFormat.format(i));
    Path file = new Path(path, fileNameStr + ".gz");
    try {
      fs.create(file);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return file;
  }

  protected void createData(FileSystem fs, Path dir, Date date,
      String streamName, String clusterName, int numFiles, int incrementNumber,
      boolean createDuplicates)
          throws IOException {
    Path path = CalendarHelper.getPathFromDate(date, dir);
    for (int i = 1; i <= numFiles; i = i + incrementNumber) {
      createFiles(fs, date, streamName, clusterName, path, i);
      if(createDuplicates) {
        Date nextDate = CalendarHelper.addAMinute(date);
        Path duplicatePath = CalendarHelper.getPathFromDate(nextDate, dir);
        duplicateFiles.add(
            createFiles(fs, date, streamName, clusterName, duplicatePath, i));
      }
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

  protected void cleanUp(DatabusConfig config) throws IOException {
    for (Cluster cluster : config.getClusters().values()) {
      FileSystem fs = FileSystem.get(cluster.getHadoopConf());
      fs.delete(new Path(cluster.getRootDir()), true);
    }
  }
}
