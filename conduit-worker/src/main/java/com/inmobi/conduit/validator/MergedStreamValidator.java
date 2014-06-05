package com.inmobi.conduit.validator;

import java.io.File;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.inmobi.conduit.utils.ParallelRecursiveListing;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.inmobi.conduit.Cluster;
import com.inmobi.conduit.ConduitConfig;
import com.inmobi.conduit.distcp.MergedStreamService;

public class MergedStreamValidator extends AbstractStreamValidator {
  private static final Log LOG = LogFactory.getLog(MergedStreamValidator.class);
  private ConduitConfig conduitConfig = null;
  private String streamName = null;
  private boolean fix = false;
  List<Path> duplicateFiles = new ArrayList<Path>();
  List<Path> holesInMerge = new ArrayList<Path>();

  List<Path> holesInLocal = new ArrayList<Path>();

  List<Cluster> srcClusterList = new ArrayList<Cluster>();
  Cluster mergeCluster = null;
  private Date startTime = null;
  private Date stopTime = null;
  private int numThreads;

  public MergedStreamValidator(ConduitConfig conduitConfig, String streamName,
      String clusterName, boolean fix, Date startTime, Date stopTime,
      int numThreads) {
    this.conduitConfig = conduitConfig;
    this.streamName = streamName;
    this.fix = fix;
    for (Cluster cluster : conduitConfig.getClusters().values()) {
      if (cluster.getSourceStreams().contains(streamName)) {
        srcClusterList.add(cluster);
      }
    }
    mergeCluster = conduitConfig.getClusters().get(clusterName);
    this.startTime = startTime;
    this.stopTime = stopTime;
    this.numThreads = numThreads;
  }

  public void execute() throws Exception {
    // check whether the given start time is beyond retention period for all 
    // source clusters
    for (Cluster srcCluster : srcClusterList) {
      validateStartTime(srcCluster);
    }
    // perform recursive listing on merge cluster
    Map<String, FileStatus> mergeStreamFileListing = new TreeMap<String, FileStatus>();
    Path mergePath = new Path(mergeCluster.getFinalDestDirRoot(), streamName);
    FileSystem mergedFs = FileSystem.get(mergeCluster.getHadoopConf());
    Path mergeStartPath = new Path(mergePath,
        Cluster.getDateAsYYYYMMDDHHMNPath(startTime));
    ParallelRecursiveListing mergeParallelListing =
        new ParallelRecursiveListing(numThreads, mergeStartPath, null);
    List<FileStatus> mergeStreamFileStatuses = 
        mergeParallelListing.getListing(mergePath, mergedFs, true);

    //find duplicates on merged cluster
    findDuplicates(mergeStreamFileStatuses, mergeStreamFileListing);
    if (duplicateFiles.isEmpty()) {
      System.out.println("No duplicate files found on cluster ["
          + mergeCluster.getName() + "] for Merged stream " + streamName);
    }
    // find holes on merge cluster
    holesInMerge.addAll(findHoles(mergeStreamFileStatuses, mergePath));
    if (!holesInMerge.isEmpty()) {
      System.out.println("holes in [ " + mergeCluster.getName() + " ] " + holesInMerge);
    } else {
      System.out.println("No holes found on cluster [" + mergeCluster.getName()
          + "] for Merged stream " + streamName);
    }

    Map<String, FileStatus> localStreamFileListing = new TreeMap<String, FileStatus>();
    // perform recursive listing on each source cluster
    for (Cluster srcCluster : srcClusterList) {
      Path localStreamPath = new Path(srcCluster.getReadLocalFinalDestDirRoot(),
          streamName);
      FileSystem localFs = localStreamPath.getFileSystem(new Configuration());
      Path startPath = new Path(localStreamPath,
          Cluster.getDateAsYYYYMMDDHHMNPath(startTime));
      Path endPath = new Path(localStreamPath,
          Cluster.getDateAsYYYYMMDDHHMNPath(stopTime));

      ParallelRecursiveListing localParallelListing =
          new ParallelRecursiveListing(numThreads, startPath, endPath);
      List<FileStatus> localStreamFileStatuses =
          localParallelListing.getListing(localStreamPath, localFs, true);

      //find holes on source cluster
      List<Path> holesInLocalCluster = findHoles(localStreamFileStatuses,
          localStreamPath);
      if (!holesInLocalCluster.isEmpty()) {
        holesInLocal.addAll(holesInLocalCluster);
        System.out.println("holes in [ " + srcCluster.getName() + " ] "
            + holesInLocalCluster);
      } else {
        System.out.println("No holes found on cluster [" + srcCluster.getName()
            + "] for local stream " + streamName);
      }
      convertListToMap(localStreamFileStatuses, localStreamFileListing);
      //find missing paths on merge cluster
      findMissingPaths(localStreamFileListing, mergeStreamFileListing);
      if (missingPaths.isEmpty()) {
        System.out.println("No missing paths found on cluster [" +
            mergeCluster.getName() + "] for Merged stream " + streamName);
      }

      if (fix && !missingPaths.isEmpty()) {
        copyMissingPaths(srcCluster);
        // clear missing paths list
        missingPaths.clear();
      }
      // clear the localStream file list map
      localStreamFileListing.clear();
    }
    if (fix && !holesInMerge.isEmpty()) {
      fixHoles(holesInMerge, mergedFs);
    }
  }

  protected void convertListToMap(List<FileStatus> listOfFileStatuses,
      Map<String, FileStatus> streamListingMap) {
    for (FileStatus file : listOfFileStatuses) {
      if (!file.isDir()) {
        streamListingMap.put(file.getPath().getName(), file);
      }
    }
  }

  protected void findDuplicates(List<FileStatus> listOfFileStatuses,
      Map<String, FileStatus> streamListingMap) {
    String fileName;
    for (FileStatus fileStatus : listOfFileStatuses) {
      if (fileStatus.isDir()) {
        continue;
      }
      fileName = fileStatus.getPath().getName();
      if (streamListingMap.containsKey(fileName)) {
        Path duplicatePath;
        Path existingPath = streamListingMap.get(fileName).getPath();
        Path currentPath = fileStatus.getPath();
        if (existingPath.compareTo(currentPath) < 0) {
          duplicatePath = currentPath;
        } else {
          duplicatePath = existingPath;
          // insert this entry into the map because this file was created first
          streamListingMap.put(fileName, fileStatus);
        }
        duplicateFiles.add(duplicatePath);
        System.out.println("Duplicate file " + duplicatePath);
      } else {
        streamListingMap.put(fileName, fileStatus);
      }
    }
  }

  protected void findMissingPaths(Map<String, FileStatus> srcListingMap,
      Map<String, FileStatus> destListingMap) {
    String fileName = null;
    for (Map.Entry<String, FileStatus> srcEntry : srcListingMap.entrySet()) {
      fileName = srcEntry.getKey();
      if (!destListingMap.containsKey(fileName)) {
        System.out.println("Missing path " + srcEntry.getValue().getPath());
        missingPaths.put(getFinalDestinationPath(srcEntry.getValue()),
            srcEntry.getValue());
      }
    }
  }

  private void validateStartTime(Cluster srcCluster) {
    int retentionHours = conduitConfig.getSourceStreams().
        get(streamName).getRetentionInHours(srcCluster.getName());
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.HOUR_OF_DAY, -retentionHours);
    if (cal.getTime().after(startTime)) {
      throw new IllegalArgumentException("Provided start time [" + startTime.toString()
          + "] is beyond the retention period [" + cal.getTime().toString() 
          + "] for cluster [" + srcCluster.getName() + "]");
    }
  }

  private void copyMissingPaths(Cluster srcCluster)
      throws Exception {
    // create an instance of MergedStreamFixService and invoke execute
    Set<String> streamsToProcess = new HashSet<String>();
    streamsToProcess.add(streamName);
    MergedStreamFixService mergeFixService = new MergedStreamFixService(
        conduitConfig, srcCluster, mergeCluster, streamsToProcess);
    // copy the missing paths through distcp and commit the copied paths
    mergeFixService.execute();
  }

  @Override
  protected String getFinalDestinationPath(FileStatus srcPath) {
    if (srcPath.isDir()) {
      return null;
    } else {
      String streamName = srcPath.getPath().getParent().getParent().getParent()
          .getParent().getParent().getParent().getName();
      return File.separator + streamName + File.separator
          + srcPath.getPath().getName();
    }
  }

  public List<Path> getDuplicateFiles() {
    return duplicateFiles;
  }

  public List<Path> getHolesInLocal() {
    return holesInLocal;
  }

  public List<Path> getHolesInMerge() {
    return holesInMerge;
  }

  class MergedStreamFixService extends MergedStreamService {
    public MergedStreamFixService(ConduitConfig config, Cluster srcCluster,
        Cluster destinationCluster, Set<String> streamsToProcess)
            throws Exception {
      super(config, srcCluster, destinationCluster, null, null,
          streamsToProcess);
    }

    @Override
    protected Path getDistCpTargetPath() {
      return new Path(getDestCluster().getTmpPath(),
          "distcp_mergedStream_fix_" + getSrcCluster().getName() + "_"
              + getDestCluster().getName() + "_"
              + getServiceName(streamsToProcess)).makeQualified(getDestFs());
    }

    @Override
    protected Map<String, FileStatus> getDistCPInputFile() throws Exception {
      return missingPaths;
    }

    @Override
    public void execute() throws Exception {
      super.execute();
    }

    @Override
    protected void finalizeCheckPoints() {
      System.out.println("Skipping update of checkpoints in Merge Stream Fix Service run");
    }
  }
}