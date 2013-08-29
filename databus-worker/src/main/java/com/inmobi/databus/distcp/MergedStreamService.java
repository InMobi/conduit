/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.inmobi.databus.distcp;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.inmobi.databus.CheckpointProvider;
import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.utils.CalendarHelper;
import com.inmobi.databus.utils.DatePathComparator;
import com.inmobi.databus.utils.FileUtil;

/*
 * Handles MergedStreams for a Cluster
 */

public class MergedStreamService extends DistcpBaseService {

  private static final Log LOG = LogFactory.getLog(MergedStreamService.class);


  public MergedStreamService(DatabusConfig config, Cluster srcCluster,
      Cluster destinationCluster, Cluster currentCluster,
      CheckpointProvider provider, Set<String> streamsToProcess)
          throws Exception {
    super(config, "MergedStreamService_" + getServiceName(streamsToProcess),
        srcCluster, destinationCluster, currentCluster, provider,
        streamsToProcess);
  }

  @Override
  protected Path getDistCpTargetPath() {
    return new Path(getDestCluster().getTmpPath(),
        "distcp_mergedStream_" + getSrcCluster().getName() + "_"
        + getDestCluster().getName() + "_"
        + getServiceName(streamsToProcess)).makeQualified(getDestFs());
  }
  
  @Override
  public void execute() throws Exception {
    LOG.info("Starting a run of service " + getName());
    try {
      boolean skipCommit = false;

      Path tmpOut = getDistCpTargetPath();
      // CleanuptmpOut before every run
      if (getDestFs().exists(tmpOut))
        getDestFs().delete(tmpOut, true);
      if (!getDestFs().mkdirs(tmpOut)) {
        LOG.warn("Cannot create [" + tmpOut + "]..skipping this run");
        return;
      }

      synchronized (getDestCluster()) {
        long commitTime = getDestCluster().getCommitTime();
        publishMissingPaths(commitTime, streamsToProcess);
      }

      Map<String, FileStatus> fileListingMap = getDistCPInputFile();
      if (fileListingMap.size() == 0) {
        LOG.warn("No data to pull from " + "Cluster ["
            + getSrcCluster().getHdfsUrl() + "]" + " to Cluster ["
            + getDestCluster().getHdfsUrl() + "]");
        finalizeCheckPoints();
        return;
      }
      LOG.info("Starting a distcp pull from Cluster ["
          + getSrcCluster().getHdfsUrl() + "]" + " to Cluster ["
          + getDestCluster().getHdfsUrl() + "] " + " Path ["
          + tmpOut.toString() + "]");

      try {
        if (!executeDistCp(getName(), fileListingMap, tmpOut))
          skipCommit = true;
      } catch (Throwable e) {
        LOG.warn("Error in distcp", e);
        LOG.warn("Problem in MergedStream distcp PULL..skipping commit for this run");
        skipCommit = true;
      }
      Map<Path, Path> commitPaths = new HashMap<Path, Path>();
      // if success
      if (!skipCommit) {
        Map<String, List<Path>> categoriesToCommit = prepareForCommit(tmpOut);
        synchronized (getDestCluster()) {
          long commitTime = getDestCluster().getCommitTime();
          // between the last addPublishMissinPaths and this call,distcp is
          // called which is a MR job and can take time hence this call ensures
          // all missing paths are added till this time
          publishMissingPaths(commitTime, streamsToProcess);
          commitPaths = createLocalCommitPaths(tmpOut, commitTime,
              categoriesToCommit);
          // category, Set of Paths to commit
          doLocalCommit(commitPaths);
        }
        finalizeCheckPoints();
      }
      // rmv tmpOut cleanup
      getDestFs().delete(tmpOut, true);
      LOG.debug("Deleting [" + tmpOut + "]");
    } catch (Exception e) {
      LOG.warn("Error in run ", e);
      throw new Exception(e);
    }
  }

  private void publishMissingPaths(long commitTime,
      Set<String> categoriesToCommit) throws Exception {
    publishMissingPaths(getDestFs(),
        getDestCluster().getFinalDestDirRoot(), commitTime, categoriesToCommit);
  }

  private Map<String, List<Path>> prepareForCommit(Path tmpOut)
      throws Exception {
    Map<String, List<Path>> categoriesToCommit = new HashMap<String, List<Path>>();
    FileStatus[] allFiles = getDestFs().listStatus(tmpOut);
    if (allFiles != null) {
      for (int i = 0; i < allFiles.length; i++) {
        String fileName = allFiles[i].getPath().getName();
        if (fileName != null) {
          String category = getCategoryFromFileName(fileName, streamsToProcess);
          if (category != null) {
            Path intermediatePath = new Path(tmpOut, category);
            if (!getDestFs().exists(intermediatePath))
              getDestFs().mkdirs(intermediatePath);
            Path source = allFiles[i].getPath().makeQualified(getDestFs());

            Path intermediateFilePath = new Path(intermediatePath
                .makeQualified(getDestFs()).toString()
                + File.separator
                + fileName);
            if (getDestFs().rename(source, intermediateFilePath) == false) {
              LOG.warn("Failed to Rename [" + source + "] to ["
                  + intermediateFilePath + "]");
              LOG.warn("Aborting Tranasction prepareForCommit to avoid data "
                  + "LOSS. Retry would happen in next run");
              throw new Exception("Rename [" + source + "] to ["
                  + intermediateFilePath + "]");
            }
            LOG.debug("Moving [" + source + "] to intermediateFilePath ["
                + intermediateFilePath + "]");
            List<Path> fileList = categoriesToCommit.get(category);
            if (fileList == null) {
              fileList = new ArrayList<Path>();
              fileList.add(intermediateFilePath.makeQualified(getDestFs()));
              categoriesToCommit.put(category, fileList);
            } else {
              fileList.add(intermediateFilePath);
            }
          }
        }
      }
    }
    return categoriesToCommit;
  }

  /*
   * @returns Map<Path, Path> - Map of filePath, destinationPath committed for
   * stream destinationPath :
   * hdfsUrl/<rootdir>/streams/<category>/YYYY/MM/HH/MN/filename
   */
  public Map<Path, Path> createLocalCommitPaths(Path tmpOut, long commitTime,
      Map<String, List<Path>> categoriesToCommit) throws Exception {
    FileSystem fs = FileSystem.get(getDestCluster().getHadoopConf());

    // find final destination paths
    Map<Path, Path> mvPaths = new LinkedHashMap<Path, Path>();
    Set<Map.Entry<String, List<Path>>> commitEntries = categoriesToCommit
        .entrySet();
    Iterator it = commitEntries.iterator();
    while (it.hasNext()) {
      Map.Entry<String, List<Path>> entry = (Map.Entry<String, List<Path>>) it
          .next();
      String category = entry.getKey();
      List<Path> filesInCategory = entry.getValue();
      for (Path filePath : filesInCategory) {
        Path destParentPath = new Path(getDestCluster().getFinalDestDir(
            category, commitTime));
        Path commitPath = new Path(destParentPath, filePath.getName());
        mvPaths.put(filePath, commitPath);
      }
    }
    return mvPaths;
  }

  private void doLocalCommit(Map<Path, Path> commitPaths) throws Exception {
    LOG.info("Committing " + commitPaths.size() + " paths.");
    FileSystem fs = FileSystem.get(getDestCluster().getHadoopConf());
    for (Map.Entry<Path, Path> entry : commitPaths.entrySet()) {
      LOG.info("Renaming " + entry.getKey() + " to " + entry.getValue());
      retriableMkDirs(fs, entry.getValue().getParent());
      if (retriableRename(fs, entry.getKey(), entry.getValue()) == false) {
        LOG.warn("Rename failed, aborting transaction COMMIT to avoid "
            + "dataloss. Partial data replay could happen in next run");
        throw new Exception("Abort transaction Commit. Rename failed from ["
            + entry.getKey() + "] to [" + entry.getValue() + "]");
      }
    }
  }

  protected Path getInputPath() throws IOException {
    String finalDestDir = getSrcCluster().getLocalFinalDestDirRoot();
    return new Path(finalDestDir);

  }



  private boolean isValidYYMMDDHHMMPath(Path prefix, Path path) {
    if (path.depth() < prefix.depth() + 5)
      return false;
    return true;
  }

  private void filterInvalidPaths(List<FileStatus> listOfFileStatus, Path prefix) {
    Iterator<FileStatus> iterator = listOfFileStatus.iterator();
    while (iterator.hasNext()) {
      if (!isValidYYMMDDHHMMPath(prefix, iterator.next().getPath()))
        iterator.remove();
    }
  }

  /**
   * This method would first try to find the starting directory by comparing the
   * destination files with source files. For eg: If a file
   * /streams/2013/04/22/16/40/a.gz is present on destination than a.gz would be
   * searched in the all the folders of source. One minute would be added to the
   * found path and would be returned. If we cannot compute the starting
   * directory by this way than the first path on the source would be returned.
   * Also during migration from V1 to V2 there is a possibility that some paths
   * of the last minute directory found at source may still not be copied (due
   * to ytm file) hence returning the uncopied path of last directory as well
   * 
   * @throws IOException
   */
  @Override
  protected Path getStartingDirectory(String stream,
      List<FileStatus> filesToBeCopied) throws IOException {
    LOG.info("Finding starting directory for merge stream from SrcCluster "
        + srcCluster.getName() + " to Destination cluster "
        + destCluster.getName() + " for stream " + stream);
    Path pathToBeListed = new Path(destCluster.getFinalDestDirRoot(), stream);
    List<FileStatus> destnFiles = null;
    try {
      if (getDestFs().exists(pathToBeListed)) {
        // TODO decide between removing invalid paths after recursive ls or
        // while ls
        destnFiles = recursiveListingOfDir(destCluster, pathToBeListed);
        filterInvalidPaths(destnFiles, pathToBeListed);
        Collections.sort(destnFiles, new DatePathComparator());
        LOG.debug("File found on destination after sorting for stream" + stream
            + " are " + FileUtil.toStringOfFileStatus(destnFiles));
      }
    } catch (IOException e) {
      LOG.error("Error while listing path" + pathToBeListed
          + " on destination Fs");
    }
    Path lastLocalPathOnSrc = null;
    pathToBeListed = new Path(srcCluster.getLocalFinalDestDirRoot(), stream);
    List<FileStatus> sourceFiles = null;
    try {
      if (getSrcFs().exists(pathToBeListed)) {
        sourceFiles = recursiveListingOfDir(srcCluster, pathToBeListed);
        filterInvalidPaths(sourceFiles, pathToBeListed);
        Collections.sort(sourceFiles, new DatePathComparator());
        LOG.debug("File found on source after sorting for stream" + stream
            + " are " + FileUtil.toStringOfFileStatus(sourceFiles));
      }
    } catch (IOException e) {
      LOG.error("Error while listing path" + pathToBeListed + " on source Fs");
    }

    if (destnFiles != null && sourceFiles != null) {
      for (int i = destnFiles.size() - 1; i >= 0; i--) {
        FileStatus current = destnFiles.get(i);
        if (current.isDir())
          continue;
        lastLocalPathOnSrc = searchFileInSource(current, sourceFiles);
        if (lastLocalPathOnSrc != null) {
          break;
        }
      }
    }
    if (lastLocalPathOnSrc == null) {
      /*
       * We cannot figure out the last processed local path because either
       * 1)There were no files for this stream on destination 2) None of the
       * files on destination was found on source cluster In both these cases
       * checkpointing the starting of the stream and if there are no source
       * files than checkpoint the current time
       */
      if (sourceFiles != null && sourceFiles.size() != 0) {
        FileStatus firstPath = sourceFiles.get(0);
        if (firstPath.isDir())
          lastLocalPathOnSrc = firstPath.getPath();
        else
          lastLocalPathOnSrc = firstPath.getPath().getParent();
        LOG.info("Starting directory couldn't be figured out hence returning the"
            + " first path at the source");
      } else {
        LOG.info("No start directory can be computed for stream " + stream);
        return null;
      }

    } else {
      // Starting path was computed from source files
      // checking whether last directory was completely copied
      FileStatus[] filesInLastDir = getSrcFs().listStatus(lastLocalPathOnSrc);
      for (FileStatus fileStatus : filesInLastDir) {
        if (searchFileInSource(fileStatus, destnFiles) == null) {
          // file present in source but absent in destination
          filesToBeCopied.add(fileStatus);
        }
      }
      // adding one minute to the found path to get the starting directory
      Path streamLevelLocalDir = new Path(getSrcCluster()
          .getLocalFinalDestDirRoot() + stream);
      Date date = CalendarHelper.getDateFromStreamDir(streamLevelLocalDir,
          lastLocalPathOnSrc);
      lastLocalPathOnSrc = CalendarHelper.getNextMinutePathFromDate(date,
          streamLevelLocalDir);

    }
    LOG.info("The start value is " + lastLocalPathOnSrc + " for stream "
        + stream);
    return lastLocalPathOnSrc;
  }

  /*
   * This method would search just the last part of the file in the source
   * cluster file .Expects the list to be sorted
   */
  private Path searchFileInSource(FileStatus destnPath,
      List<FileStatus> srcFiles) {
    LOG.debug("Searching path " + destnPath.getPath().toString()
        + " at the source");
    for (int i = srcFiles.size() - 1; i >= 0; i--) {
      FileStatus current = srcFiles.get(i);
      if (current.isDir())
        continue;
      if (current.getPath().getName().equals(destnPath.getPath().getName())) {
        FileStatus result = srcFiles.get(i);
        LOG.debug("Path found at " + result.getPath());
        if (!result.isDir())
          return result.getPath().getParent();
        else
          return result.getPath();
      }
    }
    LOG.debug("Path not found " + destnPath.getPath());
    return null;

  }

  private List<FileStatus> recursiveListingOfDir(Cluster cluster, Path path) {

    try {
      FileSystem currentFs = FileSystem.get(cluster.getHadoopConf());
      FileStatus streamDir = currentFs.getFileStatus(path);
      List<FileStatus> filestatus = new ArrayList<FileStatus>();
      createListing(currentFs, streamDir, filestatus);
      return filestatus;
    } catch (IOException ie) {
      LOG.error(
          "IOException while doing recursive listing to create checkpoint on cluster "
              + cluster, ie);
    }
    return null;

  }

  @Override
  protected String getFinalDestinationPath(FileStatus srcPath) {
    if (srcPath.isDir())
      return null;
    else
      return File.separator + srcPath.getPath().getName();

  }

}
