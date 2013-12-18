package com.inmobi.conduit.validator;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.inmobi.conduit.AbstractService;
import com.inmobi.conduit.Cluster;
import com.inmobi.conduit.DestinationStream;
import com.inmobi.conduit.distcp.MirrorStreamService;
import com.inmobi.conduit.utils.ParallelRecursiveListing;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.inmobi.conduit.DatabusConfig;

public class MirrorStreamValidator extends AbstractStreamValidator {
  private static final Log LOG = LogFactory.getLog(MirrorStreamValidator.class);
  private DatabusConfig databusConfig = null;
  private String streamName = null;
  private boolean fix = false;
  List<Path> holesInMerge = new ArrayList<Path>();
  List<Path> holesInMirror = new ArrayList<Path>();

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

    // perform parallel recursive listing of paths in source cluster
    Path mergedPath = new Path(mergedCluster.getReadFinalDestDirRoot(), streamName);
    FileSystem mergedFs = mergedPath.getFileSystem(new Configuration());
    Path startPath = getstartPath(mergedPath);
    Path endPath = getEndPath(mergedPath);
    ParallelRecursiveListing mergeParallelListing =
        new ParallelRecursiveListing(numThreads, startPath, endPath);
    List<FileStatus> mergedStreamFiles = mergeParallelListing.getListing(
        mergedPath, mergedFs, true);
    
    //find holes in source cluster
    holesInMerge.addAll(findHoles(mergedStreamFiles, mergedPath));
    if (!holesInMerge.isEmpty()) {
      System.out.println("holes in [ " + mergedCluster.getName() + " ] " + holesInMerge);
    } else {
      System.out.println("No holes found on cluster [" + mergedCluster.getName()
          + "] for  merged stream " + streamName);
    }

    // perform parallel recursive listing of paths in destination cluster
    Path mirrorPath = new Path(mirrorCluster.getFinalDestDirRoot(), streamName);
    FileSystem mirrorFs = FileSystem.get(mirrorCluster.getHadoopConf());
    startPath = getstartPath(mirrorPath);
    endPath = getEndPath(mirrorPath);
    ParallelRecursiveListing mirrorParallelListing =
        new ParallelRecursiveListing(numThreads, startPath, endPath);
    List<FileStatus> mirrorStreamFiles = mirrorParallelListing.getListing(
        mirrorPath, mirrorFs, true);
    
    // find holes in destination cluster
    holesInMirror.addAll(findHoles(mirrorStreamFiles, mirrorPath));
    if (!holesInMirror.isEmpty()) {
      System.out.println("holes in [ " + mirrorCluster.getName() + " ] " + holesInMirror);
    } else {
      System.out.println("No holes found on cluster [" + mirrorCluster.getName()
          + "] for  mirror stream " + streamName);
    }

    // find the missing and extra paths 
    findMissingAndExtraPaths(mergedStreamFiles, mirrorStreamFiles);
    if (missingPaths.isEmpty()) {
      System.out.println("No missing paths on cluster [" + mirrorCluster.getName()
          + "] for  mirror stream " + streamName);
    }

    // check if there are missing paths that need to be copied to mirror stream
    if (fix) {
      if (!holesInMerge.isEmpty()) {
        throw new IllegalStateException("Holes found in source of mirror stream." +
            " Fix holes in source cluster before fixing mirror stream");
      }

      if (!missingPaths.isEmpty()) {
        System.out.println("Number of missing paths to be copied: " + missingPaths.size());      
        // copy the missing paths
        copyMissingPaths();
      }
    }
  }

  protected void findMissingAndExtraPaths(List<FileStatus> mergedStreamFiles,
      List<FileStatus> mirrorStreamFiles) {
    Map<String, FileStatus> mergeStreamListing = new TreeMap<String, FileStatus>();
    Map<String, FileStatus> mirrorStreamListing = new TreeMap<String, FileStatus>();
    prepareMapFromList(mergedStreamFiles, mergeStreamListing, mergedCluster);
    prepareMapFromList(mirrorStreamFiles, mirrorStreamListing, mirrorCluster);
    String fileName = null;
    for (Map.Entry<String, FileStatus> srcEntry : mergeStreamListing.entrySet()) {
      fileName = srcEntry.getKey();
      if (!mirrorStreamListing.containsKey(fileName)) {
        FileStatus srcFileStatus = srcEntry.getValue();
        System.out.println("Missing path " + srcFileStatus.getPath());
        missingPaths.put(getFinalDestinationPath(srcFileStatus), srcFileStatus);
      }
    }

    for (Map.Entry<String, FileStatus> destEntry : mirrorStreamListing.entrySet()) {
      fileName = destEntry.getKey();
      if (!mergeStreamListing.containsKey(fileName)) {
        System.out.println("Extra file present in mirror stream " +
            destEntry.getValue().getPath());
      }
    }
  }

  private void prepareMapFromList(List<FileStatus> streamFiles,
      Map<String, FileStatus> srcListingMap, Cluster cluster) {
    String mergeRootDir = cluster.getRootDir();
    int mergeRootDirLen = mergeRootDir.length();
    for (FileStatus fileStatus : streamFiles) {
      String fileNameKey = fileStatus.getPath().toString().substring(
          mergeRootDirLen);
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
      throw new IllegalArgumentException("Provided start time [" + startTime.toString()
          + "] is beyond the retention period [" + cal.getTime().toString()
          + "] for cluster [" + cluster.getName() + "]");
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

  public List<Path> getHolesInMerge() {
    return holesInMerge;
  }

  public List<Path> getHolesInMirror() {
    return holesInMirror;
  }

  class MirrorStreamFixService extends MirrorStreamService {
    public MirrorStreamFixService(DatabusConfig databusConfig, Cluster srcCluster,
        Cluster destCluster, Set<String> streamsToProcess) throws Exception {
      super(databusConfig, srcCluster, destCluster, null, null,
          streamsToProcess);
    }

    @Override
    protected Path getDistCpTargetPath() {
      // create a separate path for mirror stream fix service so that it doesn't
      // interfere with the path created for mirror stream distcp
      return new Path(getDestCluster().getTmpPath(), "distcp_mirror_fix"
          + getSrcCluster().getName() + "_" + getDestCluster().getName() + "_"
          + AbstractService.getServiceName(streamsToProcess)).makeQualified(getDestFs());
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
      System.out.println("Skipping update of checkpoints in Mirror Stream Fix Service run");
    }
  }
}
