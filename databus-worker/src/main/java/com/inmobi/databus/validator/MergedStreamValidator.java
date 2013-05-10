package com.inmobi.databus.validator;

import java.io.File;
import java.util.ArrayList;
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
import com.inmobi.databus.distcp.MergedStreamService;
import com.inmobi.databus.utils.ParallelRecursiveListing;

public class MergedStreamValidator extends AbstractStreamValidator {
  private static final Log LOG = LogFactory.getLog(MergedStreamValidator.class);
  private DatabusConfig databusConfig = null;
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

  public MergedStreamValidator(DatabusConfig databusConfig, String streamName,
      String clusterName, boolean fix, Date startTime, Date stopTime,
      int numThreads) {
    this.databusConfig = databusConfig;
    this.streamName = streamName;
    this.fix = fix;
    for (Cluster cluster : databusConfig.getClusters().values()) {
      if (cluster.getSourceStreams().contains(streamName)) {
        srcClusterList.add(cluster);
      }
    }
    mergeCluster = databusConfig.getClusters().get(clusterName);
    this.startTime = startTime;
    this.stopTime = stopTime;
    this.numThreads = numThreads;
  }

  public void execute() throws Exception {
    // perform recursive listing on merge cluster
    Map<String, FileStatus> mergeStreamFileListing = new TreeMap<String, FileStatus>();
    Path mergePath = new Path(mergeCluster.getFinalDestDirRoot(), streamName);
    FileSystem mergedFs = FileSystem.get(mergeCluster.getHadoopConf());
    ParallelRecursiveListing mergeParallelListing = 
        new ParallelRecursiveListing(numThreads, null, null);
    List<FileStatus> mergeStreamFileSttuses = 
        mergeParallelListing.getListing(mergePath, mergedFs, false);
    List<FileStatus> listOfAllFilesForHolesCheck = 
        listAllFilesInSteamForFindingHoles(mergePath, mergedFs,
            mergeParallelListing);
    //find duplicates on merged cluster
    findDuplicates(mergeStreamFileSttuses, mergeStreamFileListing);

    holesInMerge.addAll(findHoles(listOfAllFilesForHolesCheck, mergePath, false,
        mergedFs));
    boolean fillHolesInMegreCluster = true;

    Map<String, FileStatus> localStreamFileListing = new TreeMap<String, FileStatus>();
    // perform recursive listing on each source cluster
    for (Cluster srcCluster : srcClusterList) {
      Path localStreamPath = new Path(srcCluster.getLocalFinalDestDirRoot(),
          streamName);
      // check whether the given start time is beyond retention period
      validateStartTime(srcCluster);
      FileSystem localFs = FileSystem.get(srcCluster.getHadoopConf());
      Path startPath = new Path(localStreamPath,
          Cluster.getDateAsYYYYMMDDHHMNPath(startTime));
      Path endPath = new Path(localStreamPath,
          Cluster.getDateAsYYYYMMDDHHMNPath(stopTime));

      ParallelRecursiveListing localParallelListing =
          new ParallelRecursiveListing(numThreads, startPath, endPath);
      List<FileStatus> localStreamFileStatuses =
          localParallelListing.getListing(localStreamPath, localFs, false);
      findDuplicates(localStreamFileStatuses, localStreamFileListing);
      List<FileStatus> listOfAllLocalFilesForHolesCheck = 
          listAllFilesInSteamForFindingHoles(localStreamPath, localFs,
              localParallelListing);

      holesInLocal.addAll(findHoles(listOfAllLocalFilesForHolesCheck,
          localStreamPath, false, localFs));

      findMissingPaths(localStreamFileListing, mergeStreamFileListing);

      if (!missingPaths.isEmpty() && fix) {
        copyMissingPaths(srcCluster);
        holesInLocal = findHoles(listOfAllLocalFilesForHolesCheck, localStreamPath, true, localFs);
        if (fillHolesInMegreCluster) {
          holesInMerge = findHoles(listOfAllFilesForHolesCheck, mergePath, true, mergedFs);
          fillHolesInMegreCluster = false;
        }
        // clear missing paths list
        missingPaths.clear();
      }
      // clear the localStream file list map
      localStreamFileListing.clear();
    }   
  }

  private List<FileStatus> listAllFilesInSteamForFindingHoles(Path mergePath,
      FileSystem mergedFs, ParallelRecursiveListing mergeParallelListing) {
    return mergeParallelListing.getListing(mergePath, mergedFs, true);
  }

  protected void findDuplicates(List<FileStatus> listOfFileStatuses,
      Map<String, FileStatus> streamListingMap) {
    String fileName;
    for (FileStatus fileStatus : listOfFileStatuses) {
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
        LOG.debug("Duplicate file " + duplicatePath);
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
        LOG.debug("Missing path " + srcEntry.getValue().getPath());
        missingPaths.put(getFinalDestinationPath(srcEntry.getValue()),
            srcEntry.getValue());
      }
    }
  }

  private void validateStartTime(Cluster srcCluster) {
    int retentionHours = databusConfig.getSourceStreams().
        get(streamName).getRetentionInHours(srcCluster.getName());
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.HOUR_OF_DAY, -retentionHours);
    if (cal.getTime().after(startTime)) {
      throw new IllegalArgumentException("provided start time is" +
          " invalid(i.e. beyond the retention period) ");
    }
  }

  private void copyMissingPaths(Cluster srcCluster)
      throws Exception {
    // create an instance of MergedStreamFixService and invoke execute
    Set<String> streamsToProcess = new HashSet<String>();
    streamsToProcess.add(streamName);
    MergedStreamFixService mergeFixService = new MergedStreamFixService(
        databusConfig, srcCluster, mergeCluster, streamsToProcess);
    // copy the missing paths through distcp and commit the copied paths
    mergeFixService.execute();
  }

  @Override
  protected String getFinalDestinationPath(FileStatus srcPath) {
    if (srcPath.isDir())
      return null;
    else
      return File.separator + srcPath.getPath().getName();

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
    public MergedStreamFixService(DatabusConfig config, Cluster srcCluster,
        Cluster destinationCluster, Set<String> streamsToProcess)
            throws Exception {
      super(config, srcCluster, destinationCluster, null, null, streamsToProcess);
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
      LOG.debug("Skipping update of checkpoints in Merge Stream Fix Service run");
    }
  }
}