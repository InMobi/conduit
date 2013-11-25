package com.inmobi.databus.validator;

import java.io.IOException;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.ReflectionUtils;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.DatabusConfigParser;
import com.inmobi.databus.DatabusConstants;
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
    System.setProperty(DatabusConstants.AUDIT_ENABLED_KEY, "true");
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

  protected Path createFile(FileSystem fs, Date date, String streamName,
      String clusterName, Path path, int i) throws IOException {
    String fileNameStr = new String(clusterName + "-" + streamName + "-" +
        getDateAsYYYYMMDDHHmm(date)+ "_" + idFormat.format(i));
    Path file = new Path(path, fileNameStr + ".gz");
    Compressor gzipCompressor = null;
    try {
      GzipCodec gzipCodec = ReflectionUtils.newInstance(GzipCodec.class,
          new Configuration());
      gzipCompressor = CodecPool.getCompressor(gzipCodec);
      FSDataOutputStream out = fs.create(file);
      OutputStream compressedOut = gzipCodec.createOutputStream(out,
          gzipCompressor);
      compressedOut.close();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (gzipCompressor != null)
        CodecPool.returnCompressor(gzipCompressor);
    }
    return file;
  }

  protected void createData(FileSystem fs, Path dir, Date date,
      String streamName, String clusterName, int numFiles, int incrementNumber,
      boolean createDuplicates)
          throws IOException {
    Path path = CalendarHelper.getPathFromDate(date, dir);
    for (int i = 1; i <= numFiles; i = i + incrementNumber) {
      createFile(fs, date, streamName, clusterName, path, i);
      if(createDuplicates) {
        Date nextDate = CalendarHelper.addAMinute(date);
        Path duplicatePath = CalendarHelper.getPathFromDate(nextDate, dir);
        duplicateFiles.add(
            createFile(fs, date, streamName, clusterName, duplicatePath, i));
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
