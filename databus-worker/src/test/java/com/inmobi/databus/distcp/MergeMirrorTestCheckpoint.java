package com.inmobi.databus.distcp;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.DatabusConfigParser;
import com.inmobi.databus.SourceStream;
import com.inmobi.databus.utils.CalendarHelper;
import com.inmobi.databus.utils.DatePathComparator;

public class MergeMirrorTestCheckpoint {

  private static final Log LOG = LogFactory
      .getLog(MergeMirrorTestCheckpoint.class);
  private static final NumberFormat idFormat = NumberFormat.getInstance();
  static {
    idFormat.setGroupingUsed(false);
    idFormat.setMinimumIntegerDigits(5);
  }


  private static String getDateAsYYYYMMDDHHmm(Date date) {
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
    return dateFormat.format(date);
  }

  private List<Path> createData(FileSystem fs, Path dir, Date date,
      String streamName, String clusterName) {
    Path path = CalendarHelper.getPathFromDate(date, dir);
    List<Path> paths = new ArrayList<Path>();
    String filenameStr1 = new String(clusterName + "-" + streamName + "-"
        + getDateAsYYYYMMDDHHmm(date) + "_" + idFormat.format(1));
    String filenameStr2 = new String(clusterName + "-" + streamName + "-"
        + getDateAsYYYYMMDDHHmm(date) + "_" + idFormat.format(2));
    Path file1 = new Path(path, filenameStr1 + ".gz");
    Path file2 = new Path(path, filenameStr2 + ".gz");
    paths.add(file1);
    paths.add(file2);
    try {
      fs.create(file1);
      fs.create(file2);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return paths;
  }

  private Map<String, List<Path>> createLocalData(DatabusConfig config)
      throws IOException {
    Date date = new Date();
    Map<String, Set<String>> sourceClusters = new HashMap<String, Set<String>>();
    for (SourceStream stream : config.getSourceStreams().values()) {
      sourceClusters.put(stream.getName(), stream.getSourceClusters());
    }
    Map<String, List<Path>> srcClusterToPathMap = new HashMap<String, List<Path>>();
    for (String stream : sourceClusters.keySet()) {
      for (String cluster : sourceClusters.get(stream)) {
      Cluster srcCluster = config.getClusters().get(cluster);
      List<Path> paths = new ArrayList<Path>();
    FileSystem fs = FileSystem.getLocal(new Configuration());
        Path streamLevelDir = new Path(srcCluster.getLocalFinalDestDirRoot()
            + stream);
        paths.addAll(createData(fs, streamLevelDir, date, stream, cluster));
    Date nextDate = CalendarHelper.addAMinute(date);
        paths.addAll(createData(fs, streamLevelDir, nextDate, stream, cluster));
      srcClusterToPathMap.put(cluster, paths);
        // Add a dummy empty directory in the end
        Date lastDate = CalendarHelper.addAMinute(nextDate);
        fs.mkdirs(CalendarHelper.getPathFromDate(lastDate, streamLevelDir));

    }
    }
    return srcClusterToPathMap;
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

  private Map<String, List<String>> launchMergeServices(DatabusConfig config)
      throws Exception {
    List<String> sourceClusters = new ArrayList<String>();
    Map<String, List<String>> srcRemoteMergeMap = new HashMap<String, List<String>>();
    for (SourceStream stream : config.getSourceStreams().values()) {
      sourceClusters.addAll(stream.getSourceClusters());
    }
    Set<String> mergedStreamRemoteClusters = new HashSet<String>();
    Map<String, Set<String>> mergedSrcClusterToStreamsMap = new HashMap<String, Set<String>>();
    for (String cluster : sourceClusters) {
      Cluster currentCluster = config.getClusters().get(cluster);
      for (String stream : currentCluster.getPrimaryDestinationStreams()) {
        for (String cName : config.getSourceStreams().get(stream)
            .getSourceClusters()) {
          mergedStreamRemoteClusters.add(cName);
          if (mergedSrcClusterToStreamsMap.get(cName) == null) {
            Set<String> tmp = new HashSet<String>();
            tmp.add(stream);
            mergedSrcClusterToStreamsMap.put(cName, tmp);
          } else {
            mergedSrcClusterToStreamsMap.get(cName).add(stream);
          }
        }

      }
      for (String remote : mergedStreamRemoteClusters) {
        MergedStreamService service = new TestMergedStreamService(config,
            config.getClusters().get(remote), currentCluster, currentCluster,
            mergedSrcClusterToStreamsMap.get(remote));
        service.execute();
        if (!srcRemoteMergeMap.containsKey(cluster)) {
          List<String> tmp = new ArrayList<String>();
          tmp.add(cluster);
          srcRemoteMergeMap.put(remote, tmp);
        } else {
          srcRemoteMergeMap.get(remote).add(cluster);
        }
      }
      }
    return srcRemoteMergeMap;
    }

  private void assertAllPathsOnSrcPresentOnDest(
      Map<String, List<Path>> srcPathList,
      Map<String, List<String>> srcToRemote, DatabusConfig config)
      throws IOException {
    for (String src : srcPathList.keySet()) {
      for (String remote : srcToRemote.get(src)) {
        Cluster remoteCluster = config.getClusters().get(remote);
        List<FileStatus> results = new ArrayList<FileStatus>();
        FileSystem remoteFs = FileSystem.get(remoteCluster.getHadoopConf());
        FileStatus pathToBeListed = remoteFs.getFileStatus(new Path(
            remoteCluster.getFinalDestDirRoot()));
        DistcpBaseService.createListing(remoteFs, pathToBeListed, results);
        for (Path srcPath : srcPathList.get(src)) {
          boolean found = false;
          for (FileStatus destnStatus : results) {
            if (destnStatus.getPath().getName().equals(srcPath.getName())) {
              found = true;
              break;
            }
          }
          if (!found) {
            LOG.error(srcPath + " not found on destination " + remote);
            assert (false);
          }

        }
      }
    }
  }

  @Test
  public void testMergeNoCheckPointNoDataOnDest() throws Exception {
    DatabusConfig config = setup("test-mss-databus.xml");
    Map<String, List<Path>> srcPathList = createLocalData(config);
    Map<String, List<String>> srcToRemote = launchMergeServices(config);
    assertAllPathsOnSrcPresentOnDest(srcPathList, srcToRemote, config);
    // TODO assert for correct checkpoint

  }

  /**
   * Data from one of the source is present on destination and no data from
   * other source
   * 
   * @throws Exception
   */
  @Test
  public void testMergeNoCheckPointWithDataOnDest() throws Exception {
    DatabusConfig config = setup("test-mss-databus.xml");
    Map<String, List<Path>> srcPathList = createLocalData(config);
    // create one of the files which has been created on source on the
    // destination;than merge should only pull data from next directory of
    // source
    List<Path> pathsOnLocal = srcPathList.get("testcluster1");
    String fileName = pathsOnLocal.get(0).getName();
    Cluster destnCluster = config.getClusters().get("testcluster1");
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(new Date());
    calendar.add(Calendar.MINUTE, -1);
    String destDir = Cluster.getDestDir(destnCluster.getFinalDestDirRoot(),
        "test1", calendar.getTime().getTime());
    Path fileToBeCreated = new Path(destDir + File.separator + fileName);
    FileSystem remoteFs = FileSystem.get(destnCluster.getHadoopConf());
    remoteFs.create(fileToBeCreated);

    launchMergeServices(config);
    // Last directory on target should have only 6 files instead of 8 as it
    // should skip first two files from source=testcluster1

    FileStatus pathToBeListed = remoteFs.getFileStatus(new Path(destnCluster
        .getFinalDestDirRoot()));
    List<FileStatus> results = new ArrayList<FileStatus>();
    DistcpBaseService.createListing(remoteFs, pathToBeListed, results);
    assert (results.size() == 7);// 1 path was created as part of setup and 6
                                 // were copied via merge
    Collections.sort(results, new DatePathComparator());
    assert (results.get(0).getPath().equals(fileToBeCreated));
    assert (!results.get(0).getPath().getParent()
        .equals(results.get(1)
        .getPath().getParent()));// first path and other paths should be
                                 // different directories

  }

  /*
   * no distcp should be launched hence no paths on target
   */
  @Test
  public void testMergeNoCheckPointNoDataOnSource() throws Exception {
    DatabusConfig config = setup("test-mss-databus.xml");
    launchMergeServices(config);
    Cluster destnCluster = config.getClusters().get("testcluster1");
    FileSystem remoteFs = FileSystem.get(destnCluster.getHadoopConf());
    assert (!remoteFs.exists(new Path(destnCluster.getFinalDestDirRoot())));

  }

  @Test
  public void testMergeNoCheckPointSourceDataPresentInDiffDirOnDest() {

  }

  @Test
  public void testMergeWithCheckPoint() {

  }
}
