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
import java.util.HashSet;
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

/*
 * Handles MergedStreams for a Cluster
 */

public class MergedStreamService extends DistcpBaseService {

  private static final Log LOG = LogFactory.getLog(MergedStreamService.class);
  private Map<String, Set<Path>> missingDirsCommittedPaths;

  // private Set<String> primaryCategories;

  public MergedStreamService(DatabusConfig config, Cluster srcCluster,
      Cluster destinationCluster, Cluster currentCluster,
      CheckpointProvider provider, Set<String> streamsToProcess)
      throws Exception {
    super(config, "MergedStreamService_" + getServiceName(streamsToProcess),
        srcCluster, destinationCluster, currentCluster, provider,
        streamsToProcess);
  }

  @Override
  public void execute() throws Exception {
    try {
      boolean skipCommit = false;
      missingDirsCommittedPaths = new LinkedHashMap<String, Set<Path>>();

      Path tmpOut = new Path(getDestCluster().getTmpPath(),
          "distcp_mergedStream_" + getSrcCluster().getName() + "_"
              + getDestCluster().getName() + "_"
              + getServiceName(streamsToProcess)).makeQualified(getDestFs());
      // CleanuptmpOut before every run
      if (getDestFs().exists(tmpOut))
        getDestFs().delete(tmpOut, true);
      if (!getDestFs().mkdirs(tmpOut)) {
        LOG.warn("Cannot create [" + tmpOut + "]..skipping this run");
        return;
      }
      Path tmp = new Path(tmpOut, "tmp");
      if (!getDestFs().mkdirs(tmp)) {
        LOG.warn("Cannot create [" + tmp + "]..skipping this run");
        return;
      }

      synchronized (getDestCluster()) {
        /*
         * missing paths are added to mirror consumer file first and those
         * missing paths are published next. If failure occurs in the current
         * run, missing paths will be recalculated and added to the map. Even if
         * databus is restarted while publishing the missing paths or writing to
         * mirror consumer file, there will be no holes in mirror stream. The
         * only disadvantage when failure occurs before publishing missing paths
         * to dest cluster is missing paths which were calculated in the
         * previous run will be added to mirror consumer file again
         */
        long commitTime = getDestCluster().getCommitTime();
        preparePublishMissingPaths(missingDirsCommittedPaths, commitTime,
            streamsToProcess);
        if (missingDirsCommittedPaths.size() > 0) {
          LOG.info("Total number of stream for which missing paths to be published "
              + missingDirsCommittedPaths.size());
          LOG.debug("Missing paths published " + missingDirsCommittedPaths);
          commitPublishMissingPaths(getDestFs(), missingDirsCommittedPaths,
              commitTime);
        }
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
      Map<String, Set<Path>> tobeCommittedPaths = null;
      Map<Path, Path> commitPaths = new HashMap<Path, Path>();
      // if success
      if (!skipCommit) {
        Map<String, List<Path>> categoriesToCommit = prepareForCommit(tmpOut);
        tobeCommittedPaths = new HashMap<String, Set<Path>>();
        synchronized (getDestCluster()) {
          long commitTime = getDestCluster().getCommitTime();
          // between the last addPublishMissinPaths and this call,distcp is
          // called which is a MR job and can take time hence this call ensures
          // all missing paths are added till this time
          preparePublishMissingPaths(missingDirsCommittedPaths, commitTime,
              streamsToProcess);
          commitPaths = createLocalCommitPaths(tmpOut, commitTime,
              categoriesToCommit, tobeCommittedPaths);
          for (Map.Entry<String, Set<Path>> entry : missingDirsCommittedPaths
              .entrySet()) {
            Set<Path> filesList = tobeCommittedPaths.get(entry.getKey());
            if (filesList != null) {
              filesList.addAll(entry.getValue());
            } else {
              tobeCommittedPaths.put(entry.getKey(), entry.getValue());
            }
          }
          commitPublishMissingPaths(getDestFs(), missingDirsCommittedPaths,
              commitTime);
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

  private void preparePublishMissingPaths(
      Map<String, Set<Path>> missingDirsCommittedPaths, long commitTime,
      Set<String> categoriesToCommit) throws Exception {
    Map<String, Set<Path>> missingDirsforCategory = null;

    if (categoriesToCommit != null) {
      missingDirsforCategory = new HashMap<String, Set<Path>>();
      for (String category : categoriesToCommit) {
        Set<Path> missingDirectories = publishMissingPaths(getDestFs(),
            getDestCluster().getFinalDestDirRoot(), commitTime, category);
        missingDirsforCategory.put(category, missingDirectories);
      }
    } else {
      missingDirsforCategory = publishMissingPaths(getDestFs(),
          getDestCluster().getFinalDestDirRoot(), commitTime);
    }

    if (missingDirsforCategory != null) {
      for (Map.Entry<String, Set<Path>> entry : missingDirsforCategory
          .entrySet()) {
        LOG.debug("Add Missing Directories to Commit Path: "
            + entry.getValue().size());
        if (missingDirsCommittedPaths.get(entry.getKey()) != null) {
          Set<Path> missingPaths = missingDirsCommittedPaths
              .get(entry.getKey());
          missingPaths.addAll(entry.getValue());
        } else {
          missingDirsCommittedPaths.put(entry.getKey(), entry.getValue());
        }
      }
    }
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
      Map<String, List<Path>> categoriesToCommit,
      Map<String, Set<Path>> tobeCommittedPaths) throws Exception {
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
        Set<Path> commitPaths = tobeCommittedPaths.get(category);
        if (commitPaths == null) {
          commitPaths = new HashSet<Path>();
        }
        commitPaths.add(commitPath);
        tobeCommittedPaths.put(category, commitPaths);
      }
    }
    return mvPaths;
  }

  private void doLocalCommit(Map<Path, Path> commitPaths) throws Exception {
    LOG.info("Committing " + commitPaths.size() + " paths.");
    FileSystem fs = FileSystem.get(getDestCluster().getHadoopConf());
    for (Map.Entry<Path, Path> entry : commitPaths.entrySet()) {
      LOG.info("Renaming " + entry.getKey() + " to " + entry.getValue());
      fs.mkdirs(entry.getValue().getParent());
      if (fs.rename(entry.getKey(), entry.getValue()) == false) {
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

  private String toStringOfFileStatus(List<FileStatus> list) {
    StringBuffer str = new StringBuffer();
    for (FileStatus f : list) {
      str.append(f.getPath().toString() + ",");
    }
    return str.toString();
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
   */
  @Override
  protected Path getStartingDirectory(String stream) {
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
            + " are " + toStringOfFileStatus(destnFiles));
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
            + " are " + toStringOfFileStatus(sourceFiles));
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
      // Starting path was computed from source files;adding one minute to the
      // found path to get the starting directory
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
