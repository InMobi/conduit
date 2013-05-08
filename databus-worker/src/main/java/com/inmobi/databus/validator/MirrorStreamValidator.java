package com.inmobi.databus.validator;

import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.DestinationStream;
import com.inmobi.databus.distcp.MirrorStreamService;
import com.inmobi.databus.utils.ParallelRecursiveListing;

public class MirrorStreamValidator extends AbstractStreamValidator {
  private static final Log LOG = LogFactory.getLog(MirrorStreamValidator.class);
  private DatabusConfig databusConfig = null;
  private String streamName = null;
  private boolean fix = false;
  Map<String, FileStatus> missingPaths = new TreeMap<String, FileStatus>();
  /**
   * @return the missingPaths
   */
  public Map<String, FileStatus> getMissingPaths() {
    return missingPaths;
  }

  Cluster mergedCluster = null;
  Cluster mirrorCluster = null;
  private Date startTime = null;
  private Date stopTime = null;
  private int numThreads;

  public MirrorStreamValidator(DatabusConfig databusConfig, 
      String streamName, String clusterName, boolean fix, Date startTime,
      Date stopTime, int numThreads) {
    this.databusConfig = databusConfig;
    this.streamName = streamName;
    this.fix = fix;  
    // get the source cluster where merge stream service is running
    mergedCluster = databusConfig.getPrimaryClusterForDestinationStream(streamName);
    // get the dest cluster where mirror stream service is running
    mirrorCluster = databusConfig.getClusters().get(clusterName);
    this.startTime = startTime;
    this.stopTime = stopTime;
    this.numThreads = numThreads;
  }

  /**
   * list all files in the merge and mirror stream using ParallelRecursiveListing
   * Find duplicates and missing paths and run the mirrorStreamFixService to 
   * fix the missingPaths
   * @throws Exception
   */
  public void execute() throws Exception {
    // validate whether start time is older than retention period
    validateStartTime(mergedCluster);
    validateStartTime(mirrorCluster);

    // perform recursive listing of paths in source cluster
    Path mergedPath = new Path(mergedCluster.getFinalDestDirRoot(), streamName);
    Map<String, FileStatus> mergeStreamFileMap = new TreeMap<String, FileStatus>();
    FileSystem mergedFs = FileSystem.get(mergedCluster.getHadoopConf());
    Path startPath = getstartPath(mergedPath);
    Path endPath = getEndPath(mergedPath);
    /*
     * list all files in merged stream using ParallelRecursiveListing utility
     */
    ParallelRecursiveListing mergeParallelListing = 
        new ParallelRecursiveListing(numThreads, startPath, endPath);
    List<FileStatus> mergedStreamFiles = mergeParallelListing.getListing(
        mergedPath, mergedFs, true);

    // perform recursive listing of paths in target cluster
    Path mirrorPath = new Path(mirrorCluster.getFinalDestDirRoot(), streamName);
    Map<String, FileStatus> mirrorStreamFileMap = new TreeMap<String, FileStatus>();
    FileSystem mirrorFs = FileSystem.get(mirrorCluster.getHadoopConf());
    startPath = getstartPath(mirrorPath);
    endPath = getEndPath(mirrorPath);
    /*
     * list all files in mirror stream using ParallelRecursiveListing utility
     */
    ParallelRecursiveListing mirrorParallelListing =
        new ParallelRecursiveListing(numThreads, startPath, endPath);
    List<FileStatus> mirrorStreamFiles = mirrorParallelListing.getListing(
        mirrorPath, mirrorFs, true);
    String rootDir = mirrorCluster.getRootDir();

    // find the missing paths
    findMissingPaths(mergedStreamFiles, mirrorStreamFiles);

    // check if there are missing paths that need to be copied to mirror stream
    if (fix && missingPaths.size() > 0) {
      LOG.debug("Number of missing paths to be copied: " + missingPaths.size());      
      // copy the missing paths
      copyMissingPaths();
    }
  }

  protected void findMissingPaths(List<FileStatus> mergedStreamFiles,
      List<FileStatus> mirrorStreamFiles) {
    Map<String, FileStatus> srcListingMap = new TreeMap<String, FileStatus>();
    Map<String, FileStatus> destListingMap = new TreeMap<String, FileStatus>();
    prepareMapFromList(mergedStreamFiles, srcListingMap, mergedCluster);
    prepareMapFromList(mirrorStreamFiles, destListingMap, mirrorCluster);
    String fileName = null;
    for (Map.Entry<String, FileStatus> srcEntry : srcListingMap.entrySet()) {
      fileName = srcEntry.getKey();
      if (!destListingMap.containsKey(fileName)) {
        FileStatus srcFileStatus = srcEntry.getValue();
        LOG.debug("Missing path " + srcFileStatus.getPath());
        missingPaths.put(getFinalDestinationPath(srcFileStatus), srcFileStatus);
      }
    }
  }

  private void prepareMapFromList(List<FileStatus> streamFiles,
      Map<String, FileStatus> srcListingMap, Cluster cluster) {
    for (FileStatus fileStatus : streamFiles) {
      String mergeRootDir = cluster.getRootDir();
      String fileNameKey = fileStatus.getPath().toString().substring(
          mergeRootDir.length());
      srcListingMap.put(fileNameKey, fileStatus);
    }
  }

  private Path getEndPath(Path streamPath) {
    return new Path(streamPath, Cluster.getDateAsYYYYMMDDHHMNPath(
        stopTime));
  }

  private Path getstartPath(Path streamPath) {
    return new Path(streamPath, Cluster.getDateAsYYYYMMDDHHMNPath(
        startTime));
  }

  private void validateStartTime(Cluster cluster) throws Exception {
    int retentionHours = Integer.MAX_VALUE;
    Map<String, DestinationStream> destinationStreamMap = cluster
        .getDestinationStreams();
    if (destinationStreamMap.containsKey(streamName)) {
      retentionHours = destinationStreamMap.get(streamName).
          getRetentionInHours();
    }
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.HOUR_OF_DAY, -retentionHours);
    if (cal.getTime().after(startTime)) {
      throw new IllegalArgumentException("provided start time is" +
          " invalid(i.e. beyond the retention period) ");
    }
  }

  void copyMissingPaths() throws Exception {
    // create an instance of MirrorStreamFixService and invoke its execute()
    Set<String> streamsToProcess = new HashSet<String>();
    streamsToProcess.add(streamName);
    MirrorStreamFixService mirrorFixService = new MirrorStreamFixService(databusConfig,
        mergedCluster, mirrorCluster, streamsToProcess);

    // copy the missing paths through distcp and commit the copied paths
    mirrorFixService.execute();
  }

  /*
   * Full path needs to be preserved for mirror stream
   */
  @Override
  protected String getFinalDestinationPath(FileStatus srcPath) {
    return srcPath.getPath().toUri().getPath();
  }

  class MirrorStreamFixService extends MirrorStreamService {
    public MirrorStreamFixService(DatabusConfig databusConfig, Cluster srcCluster,
        Cluster destCluster, Set<String> streamsToProcess) throws Exception {
      super(databusConfig, srcCluster, destCluster, null, null, streamsToProcess);
    }

    @Override
    protected Path getDistCpTargetPath() {
      // create a separate path for mirror stream fix service so that it doesn't
      // interfere with the path created for mirror stream distcp
      return new Path(getDestCluster().getTmpPath(), "distcp_mirror_fix"
          + getSrcCluster().getName() + "_" + getDestCluster().getName() + "_"
          + getServiceName(streamsToProcess)).makeQualified(getDestFs());
    }

    @Override
    protected Map<String, FileStatus> getDistCPInputFile() throws Exception {
      return missingPaths;
    }

    public void execute() throws Exception {
      super.execute();
    }

    @Override
    protected void finalizeCheckPoints() {
      LOG.debug("Skipping update of checkpoints in Mirror Stream Fix Service run");
    }
  }
}
