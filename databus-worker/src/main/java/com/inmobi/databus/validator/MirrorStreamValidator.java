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

public class MirrorStreamValidator {
  private static final Log LOG = LogFactory.getLog(MirrorStreamValidator.class);
  private DatabusConfig databusConfig = null;
  private String streamName = null;
  private boolean fix = false;
  Map<String, FileStatus> missingPaths = new TreeMap<String, FileStatus>();
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
    findDuplicates(mergedStreamFiles, mergeStreamFileMap);
    
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
    findDuplicates(mirrorStreamFiles, mirrorStreamFileMap);
    
    // find the missing paths
    findMissingPaths(mergeStreamFileMap, mirrorStreamFileMap);
    
    // check if there are missing paths that need to be copied to mirror stream
    if (fix && missingPaths.size() > 0) {
      LOG.debug("Number of missing paths to be copied: " + missingPaths.size());      
      // copy the missing paths
      copyMissingPaths();
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
  
  private void findDuplicates(List<FileStatus> listOfFileStatuses,
      Map<String, FileStatus> streamListingMap) {
    String fileName;
    for (FileStatus currentFileStatus : listOfFileStatuses) {
      fileName = currentFileStatus.getPath().toUri().getPath();
      if (streamListingMap.containsKey(fileName)) {
        Path duplicatePath;
        Path existingPath = streamListingMap.get(fileName).getPath();
        Path currentPath = currentFileStatus.getPath();
        if (existingPath.compareTo(currentPath) < 0) {
          duplicatePath = currentPath;
        } else {
          duplicatePath = existingPath;
          // insert this entry into the map because this file was created first
          streamListingMap.put(fileName, currentFileStatus);
        }
        LOG.debug("Duplicate file " + duplicatePath);
      } else {
        streamListingMap.put(fileName, currentFileStatus);
      }
    }
  }

  private void findMissingPaths(Map<String, FileStatus> mergeStreamFileMap,
      Map<String, FileStatus> mirrorStreamFileMap) {
    String fileName = null;
    for (Map.Entry<String, FileStatus> entry : mergeStreamFileMap.entrySet()) {
      fileName = entry.getKey();
      if (!mirrorStreamFileMap.containsKey(fileName)) {
        LOG.debug("Missing path " + entry.getValue().getPath());
        missingPaths.put(fileName, entry.getValue());
      }
    }
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
      LOG.debug("Skipping update of checkpoints in mirror stream fix service run");
    }
  }
}
