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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Table;
import com.inmobi.audit.thrift.AuditMessage;
import com.inmobi.databus.CheckpointProvider;
import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.utils.CalendarHelper;
import com.inmobi.databus.utils.DatePathComparator;
import com.inmobi.databus.utils.FileUtil;
import com.inmobi.messaging.publisher.MessagePublisher;

/* Assumption - Mirror is always of a merged Stream.There is only 1 instance of a merged Stream
 * (i)   1 Mirror Thread per src DatabusConfig.Cluster from where streams need to be mirrored on destCluster
 * (ii)  Mirror stream and mergedStream can't coexist on same Cluster
 * (iii) Mirror stream and merged Stream threads don't race with each other as they work on different
 * streams based on assumption(ii)
 */

public class MirrorStreamService extends DistcpBaseService {
  private static final Log LOG = LogFactory.getLog(MirrorStreamService.class);

  public MirrorStreamService(DatabusConfig config, Cluster srcCluster,
      Cluster destinationCluster,Cluster currentCluster,CheckpointProvider provider,
      Set<String> streamsToProcess,MessagePublisher publisher) throws Exception {
    super(config, MirrorStreamService.class.getName(), srcCluster,
        destinationCluster, currentCluster,provider,streamsToProcess, publisher);
  }

  @Override
  protected Path getInputPath() throws IOException {
    String finalDestDir = getSrcCluster().getReadFinalDestDirRoot();

    return new Path(finalDestDir);
  }
  
  @Override
  protected Path getDistCpTargetPath() {
    return new Path(getDestCluster().getTmpPath(), "distcp_mirror_"
        + getSrcCluster().getName() + "_" + getDestCluster().getName() + "_"
        + getServiceName(streamsToProcess)).makeQualified(getDestFs());
  }

  @Override
  protected void execute() throws Exception {
    List<AuditMessage> auditMsgList = new ArrayList<AuditMessage>();
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

      Map<String, FileStatus> fileListingMap = getDistCPInputFile();
      if (fileListingMap.size() == 0) {
        LOG.warn("No data to pull from " + "Cluster ["
            + getSrcCluster().getReadUrl() + "]" + " to Cluster ["
            + getDestCluster().getHdfsUrl() + "]");
        finalizeCheckPoints();
        return;
      }

      LOG.info("Starting a Mirrored distcp pull from Cluster ["
          + getSrcCluster().getReadUrl() + "]" + " to Cluster ["
          + getDestCluster().getHdfsUrl() + "] " + " Path ["
          + tmpOut.toString() + "]");

      try {
        if (!executeDistCp(this.getName(), fileListingMap, tmpOut))
          skipCommit = true;
      } catch (Throwable e) {
        LOG.warn("Problem in Mirrored distcp..skipping commit for this run", e);
        skipCommit = true;
      }
      if (!skipCommit) {
        LinkedHashMap<FileStatus, Path> commitPaths = prepareForCommit(tmpOut);
        doLocalCommit(commitPaths, auditMsgList);
        finalizeCheckPoints();
      }
      getDestFs().delete(tmpOut, true);
      LOG.debug("Cleanup [" + tmpOut + "]");
    } catch (Exception e) {
      LOG.warn(e);
      LOG.warn("Error in MirrorStream Service..skipping RUN ", e);
    } finally {
      publishAuditMessages(auditMsgList);
    }
  }

  void doLocalCommit(Map<FileStatus, Path> commitPaths,
      List<AuditMessage> auditMsgList) throws Exception {
    LOG.info("Committing " + commitPaths.size() + " paths.");
    Table<String, Long, Long> parsedCounters = parseCountersFile(getDestFs());
    for (Map.Entry<FileStatus, Path> entry : commitPaths.entrySet()) {
      LOG.info("Renaming [" + entry.getKey().getPath() + "] to ["
          + entry.getValue() + "]");
      if (entry.getKey().isDir()) {
        retriableMkDirs(getDestFs(), entry.getValue());
      } else {
        if (retriableExists(getDestFs(), entry.getValue())) {
          LOG.warn("File with Path [" + entry.getValue()
              + "] already exist,hence skipping renaming operation");
          continue;
        }
        retriableMkDirs(getDestFs(), entry.getValue().getParent());
        if (retriableRename(getDestFs(), entry.getKey().getPath(),
            entry.getValue()) == false) {
          LOG.warn("Failed to rename.Aborting transaction COMMIT to avoid "
              + "data loss. Partial data replay could happen in next run");
          throw new Exception("Rename failed from [" + entry.getKey().getPath()
              + "] to [" + entry.getValue() + "]");
        }
        String streamName = getTopicNameFromDestnPath(entry.getValue());
        generateAuditMsgs(streamName, entry.getKey().getPath().getName(),
            parsedCounters, auditMsgList);
      }
    }
  }

  /*
   * @returns Map<Path, Path> commitPaths - srcPath, destPath
   * 
   * @param Path - tmpOut
   */
  LinkedHashMap<FileStatus, Path> prepareForCommit(Path tmpOut) throws Exception {
    /*
     * tmpOut would be like -
     * /databus/system/tmp/distcp_mirror_<srcCluster>_<destCluster>/ After
     * distcp paths inside tmpOut would be eg:
     *
     * /databus/system/distcp_mirror_<srcCluster>_<destCluster>
     * /databus/streams/<streamName>/2012/1/13/15/7/
     * <hostname>-<streamName>-2012-01-16-07-21_00000.gz
     *
     * tmpStreamRoot eg: /databus/system/distcp_mirror_<srcCluster>_
     * <destCluster>/databus/streams/
     */

    Path tmpStreamRoot = new Path(tmpOut.makeQualified(getDestFs()).toString()
        + File.separator + getSrcCluster().getUnqaulifiedReadUrlFinalDestDirRoot());
    LOG.debug("tmpStreamRoot [" + tmpStreamRoot + "]");

     /* tmpStreamRoot eg -
      * /databus/system/tmp/distcp_mirror_<srcCluster>_<destCluster>/databus
      * /streams/
      *
      * multiple streams can get mirrored from the same cluster
      * streams can get processed in any order but we have to retain order
      * of paths within a stream*/
    FileStatus[] fileStatuses = null;
    try {
      fileStatuses = getDestFs().listStatus(tmpStreamRoot);
    } catch (FileNotFoundException e) {
    }
    //Retain the order of commitPaths
    LinkedHashMap<FileStatus, Path> commitPaths = new LinkedHashMap<FileStatus, Path>();
    if (fileStatuses != null) {
      for (FileStatus streamRoot : fileStatuses) {
        //for each stream : list the path in order of YYYY/mm/DD/HH/MM
        LOG.debug("StreamRoot [" + streamRoot.getPath() + "] streamName [" +
        streamRoot.getPath().getName() + "]");
        List<FileStatus> streamPaths = new ArrayList<FileStatus>();
        createListing(getDestFs(), streamRoot, streamPaths);
        Collections.sort(streamPaths, new DatePathComparator());
        LOG.debug("createListing size: [" + streamPaths.size() +"]");
        createCommitPaths(commitPaths, streamPaths);
      }
    }
    return commitPaths;
  }




  private void createCommitPaths(LinkedHashMap<FileStatus, Path> commitPaths,
                                 List<FileStatus> streamPaths) {
   /*  Path eg in streamPaths -
    *  /databus/system/distcp_mirror_<srcCluster>_<destCluster>/databus/streams
    *  /<streamName>/2012/1/13/15/7/<hostname>-<streamName>-2012-01-16-07
    *  -21_00000.gz
    *
    * or it could be an emptyDir like
    *  /* Path eg in streamPaths -
    *  /databus/system/distcp_mirror_<srcCluster>_<destCluster>/databus/streams
    *  /<streamName>/2012/1/13/15/7/
    *
    */

    for (FileStatus fileStatus : streamPaths) {
      String fileName = null;

      Path prefixDir = null;
      if (fileStatus.isDir()) {
        //empty directory
        prefixDir = fileStatus.getPath();
      } else {
        fileName = fileStatus.getPath().getName();
        prefixDir = fileStatus.getPath().getParent();
      }

      Path min = prefixDir;
      Path hr =  min.getParent() ;
      Path day = hr.getParent();
      Path month = day.getParent();
      Path year = month.getParent();
      Path streamName = year.getParent();

      String finalPath = getDestCluster().getFinalDestDirRoot() + File
      .separator + streamName.getName() + File.separator + year.getName() + File
      .separator + month.getName() + File.separator + day.getName() + File
      .separator + hr.getName() + File.separator + min.getName();

      if (fileName != null) {
        finalPath += File.separator + fileName;
      }

      commitPaths.put(fileStatus, new Path(finalPath));
      LOG.debug("Going to commit [" + fileStatus.getPath() + "] to [" +
      finalPath + "]");
    }

  }

  /*
   * Method to get the starting directory in cases when checkpoint for a stream
   * is not present or is invalid. First this method would check on the
   * destination FS to compute the last mirrored path;if found would add one
   * minute to the path return its equivalent on source cluster.If not found
   * than it would check on the source cluster to compute the first merged path
   * and would return that. This method can return null in cases where its not
   * able to calculate the starting directory. Also it compares the last
   * directory on the destination with corresponding dir on source to find
   * uncopied files
   */
  @Override
  protected Path getStartingDirectory(String stream,
      List<FileStatus> filesToBeCopied) throws IOException {
    Path finalDestDir = new Path(destCluster.getFinalDestDirRoot());
    Path streamFinalDestDir = new Path(finalDestDir, stream);
    Path finalSrcDir = new Path(srcCluster.getReadFinalDestDirRoot());
    Path streamFinalSrcDir = new Path(finalSrcDir, stream);

    Path lastMirroredPath = getFirstOrLastPath(getDestFs(), streamFinalDestDir,
        true);
    Path lastMergedPathOnSrc = null;
    Path result;
    if (lastMirroredPath == null) {
      LOG.info("Cannot compute the starting directory from the destination data");
      lastMergedPathOnSrc = getFirstOrLastPath(getSrcFs(), streamFinalSrcDir,
          false);
      if (lastMergedPathOnSrc == null) {
        LOG.info("Cannot compute starting directory  from either destination or source data for stream "
            + stream);
        return null;
      } else
        result = lastMergedPathOnSrc;
    } else {
      LOG.info("Starting directory was calculated from the destination data,making the path qualified w.r.t source");
      // Path was found on destination,adding a minute to this path and making
      // it qualified w.r.t source as well

      Date date = CalendarHelper.getDateFromStreamDir(streamFinalDestDir,
          lastMirroredPath);
      Path correspondingMergePath = CalendarHelper.getPathFromDate(date,
          streamFinalSrcDir);
      List<FileStatus> files = findDifferentFiles(
          FileUtil.listStatusAsPerHDFS(getSrcFs(), correspondingMergePath),
          FileUtil.listStatusAsPerHDFS(getDestFs(), lastMirroredPath));
      if (files != null)
        filesToBeCopied.addAll(files);
      result = CalendarHelper.getNextMinutePathFromDate(date,
          streamFinalSrcDir);
    }
    return result;
  }

  /*
   * Return files which are present in first array and not present in second
   */
  private List<FileStatus> findDifferentFiles(FileStatus[] files1,
      FileStatus[] files2) {
    if (files2 == null || files2.length == 0)
      return Arrays.asList(files1);
    if (files1 == null || files1.length == 0)
      return new ArrayList<FileStatus>();
    List<FileStatus> result = new ArrayList<FileStatus>();
    for (FileStatus filestatus1 : files1) {
      boolean found = false;
      for (FileStatus filestatus2 : files2) {
        if (filestatus1.getPath().getName()
            .equalsIgnoreCase(filestatus2.getPath().getName())) {
          found = true;
          break;
        }
      }
      if (!found)
        result.add(filestatus1);
    }
    return result;
  }


  private void recursiveListingTillMinuteDir(FileSystem fs,
      FileStatus fileStatus, List<FileStatus> results, int depth)
      throws IOException {
    if (fileStatus.isDir()) {

      FileStatus[] stats = FileUtil.listStatusAsPerHDFS(fs,
          fileStatus.getPath());
      if (stats != null) {
        for (FileStatus stat : stats) {
          if (depth == 4) {
            results.add(stat);
          } else {
            recursiveListingTillMinuteDir(fs, stat, results, depth + 1);
          }
        }
      }
    }
  }

  private Path getFirstOrLastPath(FileSystem fs, Path streamFinalDestDir,
      boolean returnLast) throws IOException {
    if (!fs.exists(streamFinalDestDir))
      return null;
    FileStatus streamRoot;
    List<FileStatus> streamPaths = new ArrayList<FileStatus>();
    streamRoot = fs.getFileStatus(streamFinalDestDir);
    recursiveListingTillMinuteDir(fs, streamRoot, streamPaths, 0);
    if (streamPaths.size() == 0)
      return null;
    DatePathComparator comparator = new DatePathComparator();
    FileStatus result = streamPaths.get(0);
    for (int i = 0; i < streamPaths.size(); i++) {
      FileStatus current = streamPaths.get(i);
      if (returnLast && comparator.compare(current, result) > 0)
        result = current;
      else if (!returnLast && comparator.compare(current, result) < 0)
        result = current;
    }
    if (!result.isDir())
      return result.getPath().getParent();
    else
      return result.getPath();

  }


  @Override
  protected String getTier() {
    return "mirror";
  }
  /*
   * Full path needs to be preserved for mirror stream
   */
  @Override
  protected String getFinalDestinationPath(FileStatus srcPath) {
    return srcPath.getPath().toUri().getPath();
  }
}
