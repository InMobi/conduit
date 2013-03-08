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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;

/*
 * Handles MergedStreams for a Cluster
 */

public class MergedStreamService extends DistcpBaseService {

  private static final Log LOG = LogFactory.getLog(MergedStreamService.class);
  private Map<String, Set<Path>> missingDirsCommittedPaths;
  private Set<String> primaryCategories;

  public MergedStreamService(DatabusConfig config, Cluster srcCluster,
      Cluster destinationCluster, Cluster currentCluster) throws Exception {
    super(config, MergedStreamService.class.getName(), srcCluster,
        destinationCluster, currentCluster);
    primaryCategories = destinationCluster.getPrimaryDestinationStreams();
  }

  @Override
  public void execute() throws Exception {
    try {
      boolean skipCommit = false;
      Map<Path, FileSystem> consumePaths = new HashMap<Path, FileSystem>();
      missingDirsCommittedPaths = new HashMap<String, Set<Path>>();

      Path tmpOut = new Path(getDestCluster().getTmpPath(),
          "distcp_mergedStream_" + getSrcCluster().getName() + "_"
              + getDestCluster().getName()).makeQualified(getDestFs());
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
        /*missing paths are added to mirror consumer file first and those missing
        paths are published next. 'missingDirsCommittedPaths' map is not cleared
        until all these missing paths are successfully published. If failure 
        occurs in the current run, missing paths will be recalculated and added
        to the map. Even if databus is restarted while publishing the missing 
        paths or writing to mirror consumer file, there will be no holes in 
        mirror stream. The only disadvantage when failure occurs before 
        publishing missing paths to dest cluster is missing paths which were 
        calculated in the previous run will be added to mirror consumer file 
        again */
        preparePublishMissingPaths(missingDirsCommittedPaths, -1, primaryCategories);
        if (missingDirsCommittedPaths.size() > 0) {
          LOG.info("Adding Missing Directories to the mirror consumer file and" +
              " publishing the missing paths"+ missingDirsCommittedPaths.size());
          commitMirroredConsumerPaths(missingDirsCommittedPaths, tmp);
          commitPublishMissingPaths(getDestFs(), missingDirsCommittedPaths);
        }
      }

      Path inputFilePath = getDistCPInputFile(consumePaths, tmp);
      if (inputFilePath == null) {
        LOG.warn("No data to pull from " + "Cluster ["
            + getSrcCluster().getHdfsUrl() + "]" + " to Cluster ["
            + getDestCluster().getHdfsUrl() + "]");
        return;
      }
      LOG.warn("Starting a distcp pull from [" + inputFilePath.toString()
          + "] " + "Cluster [" + getSrcCluster().getHdfsUrl() + "]"
          + " to Cluster [" + getDestCluster().getHdfsUrl() + "] " + " Path ["
          + tmpOut.toString() + "]");

      try {
        if (!executeDistCp(getDistCpOptions(inputFilePath, tmpOut)))
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
        tobeCommittedPaths =  new HashMap<String, Set<Path>>();
        synchronized (getDestCluster()) {
          long commitTime = getDestCluster().getCommitTime();
          // between the last addPublishMissinPaths and this call,distcp is
          // called which is a MR job and can take time hence this call ensures
          // all missing paths are added till this time
          preparePublishMissingPaths(missingDirsCommittedPaths, commitTime,
              primaryCategories);
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

          // Prepare paths for MirrorStreamConsumerService
          commitMirroredConsumerPaths(tobeCommittedPaths, tmp);
          commitPublishMissingPaths(getDestFs(), missingDirsCommittedPaths);
          // category, Set of Paths to commit
          doLocalCommit(commitPaths);
        }


        // Cleanup happens in parallel without sync
        // no race is there in consumePaths, tmpOut
        doFinalCommit(consumePaths);
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
      Set<String> categoriesToCommit) 
          throws Exception {
    Map<String, Set<Path>> missingDirsforCategory = null;

    if(categoriesToCommit!=null) {
      missingDirsforCategory = new HashMap<String, Set<Path>>();
      for (String category : categoriesToCommit) {
        Set<Path> missingDirectories = publishMissingPaths(getDestFs(),
            getDestCluster().getFinalDestDirRoot(), commitTime, category);
        missingDirsforCategory.put(category, missingDirectories);
      }
    } else {
      missingDirsforCategory = publishMissingPaths(
          getDestFs(), getDestCluster().getFinalDestDirRoot());
    }

    if (missingDirsforCategory != null) {
      for (Map.Entry<String, Set<Path>> entry : missingDirsforCategory
          .entrySet()) {
        LOG.debug("Add Missing Directories to Commit Path: "
            + entry.getValue().size());
        if (missingDirsCommittedPaths.get(entry.getKey()) != null) {
          Set<Path> missingPaths = missingDirsCommittedPaths.get(entry
              .getKey());
          missingPaths.addAll(entry.getValue());
        } else {
          missingDirsCommittedPaths.put(entry.getKey(), entry.getValue());
        }
      }
    }
  }

  /*
   * @param Map<String, Set<Path>> commitedPaths - Stream Name, It's committed
   * Path.
   * tmpConsumePath: hdfsUrl/rootDir/system/tmp/
   * distcp_mergedStream_databusdev1_databusdev2/tmp/src_sourceCluster_via_
   * destinationCluster_mirrorto_consumername_streamname
   * final Mirror Path: hdfsUrl/rootDir/system/mirrors/
   * databusdev1/src_sourceCluster_via_destinationCluster_mirrorto_consumerName_
   * benchmark_merge_filestatus
   * Example paths:
   * tmpConsumePath:hdfs://databusdev2.mkhoj.com:9000/databus/system/tmp/
   * distcp_mergedStream_databusdev1_databusdev2/tmp/
   * src_databusdev1_via_databusdev2_mirrorto_databusdev1_benchmark_merge
   * 
   * finalMirrorPath: hdfs://databusdev2.mkhoj.com:9000/databus/system/mirrors/
   * databusdev1/src_databusdev1_via_databusdev2_mirrorto_databusdev1_
   * benchmark_merge_1352687040907
   */
  private void commitMirroredConsumerPaths(
      Map<String, Set<Path>> tobeCommittedPaths, Path tmp) throws Exception {
    // Map of Stream and clusters where it's mirrored
    Map<String, Set<Cluster>> mirrorStreamConsumers = new HashMap<String, 
        Set<Cluster>>();
    Map<Path, Path> mirrorCommitPaths = new LinkedHashMap<Path, Path>();
    // for each stream in committedPaths
    for (String stream : tobeCommittedPaths.keySet()) {
      // for each cluster
      for (Cluster cluster : getConfig().getClusters().values()) {
        // is this stream to be mirrored on this cluster
        if (cluster.getMirroredStreams().contains(stream)) {
          Set<Cluster> mirrorConsumers = mirrorStreamConsumers.get(stream);
          if (mirrorConsumers == null)
            mirrorConsumers = new HashSet<Cluster>();
          mirrorConsumers.add(cluster);
          mirrorStreamConsumers.put(stream, mirrorConsumers);
        }
      }
    } // for each stream

    // Commit paths for each consumer
    for (String stream : tobeCommittedPaths.keySet()) {
      // consumers for this stream
      Set<Cluster> consumers = mirrorStreamConsumers.get(stream);
      Path tmpConsumerPath;
      if (consumers == null || consumers.size() == 0) {
        LOG.warn(" Consumers is empty for stream [" + stream + "]");
        continue;
      }
      for (Cluster consumer : consumers) {
        // commit paths for this consumer, this stream
        // adding srcCluster avoids two Remote Copiers creating same filename
        String tmpPath = "src_" + getSrcCluster().getName() + "_via_"
            + getDestCluster().getName() + "_mirrorto_" + consumer.getName()
            + "_" + stream;
        tmpConsumerPath = new Path(tmp, tmpPath);
        FSDataOutputStream out = getDestFs().create(tmpConsumerPath);
        try {
          for (Path path : tobeCommittedPaths.get(stream)) {
            LOG.debug("Writing Mirror Commit Path [" + path.toString() + "]");
            out.writeBytes(path.toString());
            out.writeBytes("\n");
          }
        } finally {
          out.close();
        }
        // Two MergedStreamConsumers will write file for same consumer within
        // the same time
        // adding srcCLuster name avoids that conflict
        Path finalMirrorPath = new Path(getDestCluster().getMirrorConsumePath(
            consumer), tmpPath + "_"
                + new Long(System.currentTimeMillis()).toString());
        mirrorCommitPaths.put(tmpConsumerPath, finalMirrorPath);
      } // for each consumer
    } // for each stream
    doLocalCommit(mirrorCommitPaths);
  }

  private Map<String, List<Path>> prepareForCommit(Path tmpOut)
      throws Exception {
    Map<String, List<Path>> categoriesToCommit = new HashMap<String, List<Path>>();
    FileStatus[] allFiles = getDestFs().listStatus(tmpOut);
    if (allFiles != null) {
      for (int i = 0; i < allFiles.length; i++) {
        String fileName = allFiles[i].getPath().getName();
        if (fileName != null) {
          String category = getCategoryFromFileName(fileName,
              getDestCluster().getPrimaryDestinationStreams());
          if (category != null) {
            Path intermediatePath = new Path(tmpOut, category);
            if (!getDestFs().exists(intermediatePath))
              getDestFs().mkdirs(intermediatePath);
            Path source = allFiles[i].getPath().makeQualified(getDestFs());

            Path intermediateFilePath = new Path(
                intermediatePath.makeQualified(getDestFs()).toString() +
                    File.separator + fileName);
            if (getDestFs().rename(source, intermediateFilePath) == false) {
              LOG.warn("Failed to Rename [" + source + "] to [" +
                  intermediateFilePath + "]");
              LOG.warn("Aborting Tranasction prepareForCommit to avoid data " +
                  "LOSS. Retry would happen in next run");
              throw new Exception("Rename [" + source + "] to [" +
                  intermediateFilePath + "]");
            }
            LOG.debug("Moving [" + source + "] to intermediateFilePath [" +
                intermediateFilePath + "]");
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
   * @returns Map<Path, Path> - Map of filePath, destinationPath committed
   * for stream
   * destinationPath : hdfsUrl/rootdir/streams/category/YYYY/MM/HH/MN/filename
   * Example path:
   * hdfs://databusdev2.mkhoj.com:9000/databus/streams/test-topic/
   * 2012/10/00/00/filename
   */
  public Map<Path, Path> createLocalCommitPaths(Path tmpOut, long commitTime, 
      Map<String, List<Path>> categoriesToCommit, 
      Map<String, Set<Path>> tobeCommittedPaths) 
          throws Exception {
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
    return getSrcCluster().getConsumePath(getDestCluster());
  }

  private void removePathWithDuplicateFileNames(Set<String> minFilesSet) {
    Set<String> fileNameSet = new HashSet<String>();
    Iterator<String> iterator = minFilesSet.iterator();
    while (iterator.hasNext()) {
      Path p = new Path(iterator.next());
      if (fileNameSet.contains(p.getName())) {
        LOG.info("Removing duplicate path [" + p + "]");
        iterator.remove();
      } else
        fileNameSet.add(p.getName());

    }
  }

  @Override
  public void filterMinFilePaths(Set<String> minFilesSet) {
    // remove all the minute file paths which have the duplicate 'filename'
    removePathWithDuplicateFileNames(minFilesSet);

  }
}
