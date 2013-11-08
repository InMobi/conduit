package com.inmobi.databus.distcp;

import java.io.IOException;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.testng.annotations.Test;

import com.inmobi.databus.AbstractService;
import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.DatabusConfigParser;
import com.inmobi.databus.DatabusConstants;
import com.inmobi.databus.DestinationStream;
import com.inmobi.databus.FSCheckpointProvider;
import com.inmobi.databus.SourceStream;
import com.inmobi.databus.utils.CalendarHelper;
import com.inmobi.databus.utils.DatePathComparator;
import com.inmobi.databus.utils.FileUtil;

public class MirrorCheckPointTest {

  private static final Log LOG = LogFactory.getLog(MirrorCheckPointTest.class);
  private static final NumberFormat idFormat = NumberFormat.getInstance();
  private Path auditUtilJarDestPath;
  private Path jarsPath;
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
    Compressor gzipCompressor = null;
    try {
      GzipCodec gzipCodec = ReflectionUtils.newInstance(GzipCodec.class,
          new Configuration());
      gzipCompressor = CodecPool.getCompressor(gzipCodec);
      FSDataOutputStream out = fs.create(file1);
      OutputStream compressedOut = gzipCodec.createOutputStream(out,
          gzipCompressor);
      compressedOut.close();

      out = fs.create(file2);
      compressedOut = gzipCodec.createOutputStream(out, gzipCompressor);
      compressedOut.close();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } finally {
      if (gzipCompressor != null)
        CodecPool.returnCompressor(gzipCompressor);
    }
    return paths;
  }

  private Map<String, List<Path>> createMergeData(DatabusConfig config)
      throws IOException {
    Date date = new Date();
    Map<String, Cluster> primaryClusters = new HashMap<String, Cluster>();
    for (SourceStream stream : config.getSourceStreams().values()) {
      primaryClusters.put(stream.getName(),
          config.getPrimaryClusterForDestinationStream(stream.getName()));
    }
    Map<String, List<Path>> srcClusterToPathMap = new HashMap<String, List<Path>>();
    for (String stream : primaryClusters.keySet()) {
      List<Path> paths = new ArrayList<Path>();
      Cluster primaryCluster = primaryClusters.get(stream);
      for (String cluster : config.getSourceStreams().get(stream)
          .getSourceClusters()) {
        FileSystem fs = FileSystem.getLocal(new Configuration());
        Path streamLevelDir = new Path(primaryCluster.getFinalDestDirRoot()
            + stream);
        paths.addAll(createData(fs, streamLevelDir, date, stream, cluster));
        Date nextDate = CalendarHelper.addAMinute(date);
        paths.addAll(createData(fs, streamLevelDir, nextDate, stream, cluster));
        // Add a dummy empty directory in the end
        Date lastDate = CalendarHelper.addAMinute(nextDate);
        fs.mkdirs(CalendarHelper.getPathFromDate(lastDate, streamLevelDir));
      }
      srcClusterToPathMap.put(primaryCluster.getName(), paths);
    }
    return srcClusterToPathMap;
  }

  private DatabusConfig setup(String configFile) throws Exception {
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

  private Map<String, List<String>> launchMirrorServices(DatabusConfig config)
      throws Exception {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    String auditSrcJar = FileUtil.findContainingJar(
        com.inmobi.messaging.util.AuditUtil.class);
    Map<String, List<String>> srcRemoteMirrorMap = new HashMap<String, List<String>>();
    Map<String, Set<String>> mirrorSrcClusterToStreamsMap = new HashMap<String, Set<String>>();
    for (Cluster cluster : config.getClusters().values()) {
      for(DestinationStream stream:cluster.getDestinationStreams().values()){
        if(!stream.isPrimary()){
          jarsPath = new Path(cluster.getTmpPath(), "jars");
          auditUtilJarDestPath = new Path(jarsPath, "messaging-client-core.jar");
          // Copy AuditUtil src jar to FS
          fs.copyFromLocalFile(new Path(auditSrcJar), auditUtilJarDestPath);
         Cluster remote = config.getPrimaryClusterForDestinationStream(stream.getName());
         if(remote!=null){
           if(mirrorSrcClusterToStreamsMap.get(remote.getName())!=null){
             mirrorSrcClusterToStreamsMap.get(remote.getName()).add(stream.getName());
           }else {
             Set<String> tmp = new HashSet<String>();
             tmp.add(stream.getName());
             mirrorSrcClusterToStreamsMap.put(remote.getName(), tmp);
           }  
         }
        }
      }
      for(String remote:mirrorSrcClusterToStreamsMap.keySet()){
        Cluster srcCluster = config.getClusters().get(remote);
        MirrorStreamService service = new TestMirrorStreamService(config,
            srcCluster, cluster, cluster,
            mirrorSrcClusterToStreamsMap.get(remote));
        service.execute();
        if(srcRemoteMirrorMap.get(remote)==null){
          List<String> tmp = new ArrayList<String>();
          tmp.add(cluster.getName());
          srcRemoteMirrorMap.put(remote, tmp);
        }else{
          srcRemoteMirrorMap.get(remote).add(cluster.getName());
        }
      }
      mirrorSrcClusterToStreamsMap.clear();
    }
    return srcRemoteMirrorMap;

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

  private List<FileStatus> pathToFileStatusList(List<Path> pathList)
      throws IOException {
    List<FileStatus> fStatusList = new ArrayList<FileStatus>();
    FileSystem fs = FileSystem.getLocal(new Configuration());
    for (Path p : pathList)
      fStatusList.add(fs.getFileStatus(p));
    Collections.sort(fStatusList, new DatePathComparator());
    return fStatusList;

  }

  @Test
  public void testMirrorNoCheckPointNoDataOnDest() throws Exception {
    DatabusConfig config = setup("test-mss-databus.xml");
    Map<String, List<Path>> srcPathList = createMergeData(config);
    Map<String, List<String>> srcToRemote = launchMirrorServices(config);
    assertAllPathsOnSrcPresentOnDest(srcPathList, srcToRemote, config);

    String checkPointKey1 = AbstractService.getCheckPointKey(
        TestMirrorStreamService.class.getSimpleName(), "test1", "testcluster1");

    Cluster destnCluster1 = config.getClusters().get("testcluster2");
    List<Path> pathsCreated1 = srcPathList.get("testcluster1");
    List<FileStatus> fStatusList = pathToFileStatusList(pathsCreated1);
    FSCheckpointProvider provider = new FSCheckpointProvider(
        destnCluster1.getCheckpointDir());
    byte[] value = provider.read(checkPointKey1);
    String checkPointString = new String(value);
    assert (fStatusList.get(7).getPath().getParent().toString()
        .equals(checkPointString));
  }

  /**
   * Data from one of the source is present on destination and no data from
   * other source
   * 
   * @throws Exception
   */
  @Test
  public void testMirrorNoCheckPointWithDataOnDest() throws Exception {
    DatabusConfig config = setup("test-mss-databus.xml");
    Map<String, List<Path>> srcPathList = createMergeData(config);
    // create one of the files which has been created on source ;on the
    // destination;than merge should only pull data from next directory of
    // source
    List<Path> pathsOnMirrorSrc = srcPathList.get("testcluster1");
    List<FileStatus> fStatusList = pathToFileStatusList(pathsOnMirrorSrc);
    Path pathToBeCreated = fStatusList.get(0).getPath();
    Cluster destnCluster = config.getClusters().get("testcluster2");
    FileSystem remoteFs = FileSystem.get(destnCluster.getHadoopConf());
    Cluster srcCluster = config.getClusters().get("testcluster1");
    String finalRelativePath = pathToBeCreated.toString();
    String srcRootDir = new Path(srcCluster.getRootDir()).toString();
    String tmp = finalRelativePath.substring(
srcRootDir.length() + 1,
        finalRelativePath.length());
    Path finalPath = remoteFs.makeQualified(new Path(destnCluster.getRootDir(),
        tmp));
    remoteFs.create(finalPath);

    launchMirrorServices(config);
    // Last directory on target should have all 8 files

    FileStatus pathToBeListed = remoteFs.getFileStatus(new Path(destnCluster
        .getFinalDestDirRoot(), "test1"));
    List<FileStatus> results = new ArrayList<FileStatus>();
    DistcpBaseService.createListing(remoteFs, pathToBeListed, results);
    assert (results.size() == 8);// 1 file was created as part of setup
    Collections.sort(results, new DatePathComparator());
    String checkPointKey1 = AbstractService.getCheckPointKey(
        TestMirrorStreamService.class.getSimpleName(), "test1", "testcluster1");
    FSCheckpointProvider provider = new FSCheckpointProvider(
        destnCluster.getCheckpointDir());
    String checkPointValue = new String(provider.read(checkPointKey1));
    assert (fStatusList.get(7).getPath().getParent().toString()
        .equals(checkPointValue));

  }
  
   /*
   * no distcp should be launched hence no paths on target
   */
  @Test
  public void testMirrorNoCheckPointNoDataOnSourceAndDest() throws Exception {
    DatabusConfig config = setup("test-mss-databus.xml");
    launchMirrorServices(config);
    Cluster destnCluster = config.getClusters().get("testcluster2");
    FileSystem remoteFs = FileSystem.get(destnCluster.getHadoopConf());
    assert (!remoteFs.exists(new Path(destnCluster.getFinalDestDirRoot())));

  }

  @Test
  public void testMirrorDataOnDestnNoDataOnSrc() throws Exception {
    DatabusConfig config = setup("test-mss-databus.xml");
    Cluster destnCluster = config.getClusters().get("testcluster2");
    String streamLevelDir = destnCluster.getFinalDestDirRoot() + "test1";
    FileSystem destFs = FileSystem.get(destnCluster.getHadoopConf());
    createData(destFs, new Path(streamLevelDir), new Date(), "test1",
        "testcluster1");
    launchMirrorServices(config);
    List<FileStatus> results = new ArrayList<FileStatus>();
    DistcpBaseService.createListing(destFs,
        destFs.getFileStatus(new Path(streamLevelDir)), results);
    assert (results.size() == 2);

  }

  @Test
  public void testMirrorWithCheckPoint() throws Exception {
    DatabusConfig config = setup("test-mss-databus.xml");
    Map<String, List<Path>> srcPathList = createMergeData(config);
    Cluster destnCluster = config.getClusters().get("testcluster2");
    List<Path> pathsCreated1 = srcPathList.get("testcluster1");
    FSCheckpointProvider provider = new FSCheckpointProvider(
        destnCluster.getCheckpointDir());
    String checkPointKey1 = AbstractService.getCheckPointKey(
        TestMirrorStreamService.class.getSimpleName(), "test1", "testcluster1");
    List<FileStatus> fStatus1 = pathToFileStatusList(pathsCreated1);
    Path checkPointPath1 = fStatus1.get(0).getPath().getParent();
    provider.checkpoint(checkPointKey1, checkPointPath1.toString().getBytes());
    launchMirrorServices(config);
    Path pathToBeListed = new Path(destnCluster.getFinalDestDirRoot()
        + "test1");

    FileSystem remoteFs = FileSystem.get(destnCluster.getHadoopConf());
    List<FileStatus> results = new ArrayList<FileStatus>();
    DistcpBaseService.createListing(remoteFs,
        remoteFs.getFileStatus(pathToBeListed), results);

    assert (results.size() == 4);

    byte[] value = provider.read(checkPointKey1);
    String checkPointString = new String(value);
    assert (fStatus1.get(7).getPath().getParent().toString()
        .equals(checkPointString));

  }

  @Test
  public void testMirrorNoChkPointEmptyDirAtDestination() throws Exception {
    DatabusConfig config = setup("test-mss-databus.xml");
    Cluster destnCluster = config.getClusters().get("testcluster2");
    FileSystem remoteFs2 = FileSystem.get(destnCluster.getHadoopConf());
    Path emptyPath = new Path(destnCluster.getFinalDestDirRoot() + "test1");
    remoteFs2.mkdirs(emptyPath);

    Map<String, List<Path>> srcPathList = createMergeData(config);
    Map<String, List<String>> srcToRemote = launchMirrorServices(config);
    assertAllPathsOnSrcPresentOnDest(srcPathList, srcToRemote, config);
    List<FileStatus> results = new ArrayList<FileStatus>();
    DistcpBaseService.createListing(remoteFs2,
        remoteFs2.getFileStatus(emptyPath), results);
    assert (results.size() == 8);

  }

}
