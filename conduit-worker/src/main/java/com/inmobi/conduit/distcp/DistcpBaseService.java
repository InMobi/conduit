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
package com.inmobi.conduit.distcp;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.inmobi.conduit.AbstractService;
import com.inmobi.conduit.utils.CalendarHelper;
import com.inmobi.conduit.utils.FileUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;

import com.inmobi.conduit.CheckpointProvider;
import com.inmobi.conduit.Cluster;
import com.inmobi.conduit.DatabusConfig;
import com.inmobi.conduit.DatabusConstants;


public abstract class DistcpBaseService extends AbstractService {

  protected final Cluster srcCluster;
  protected final Cluster destCluster;
  protected final Cluster currentCluster;
  private final FileSystem srcFs;
  private final FileSystem destFs;
  protected final CheckpointProvider provider;
  protected Map<String, Path> checkPointPaths = new HashMap<String, Path>();
  private static final int DEFAULT_NUM_DIR_PER_DISTCP_STREAM = 30;

  protected static final Log LOG = LogFactory.getLog(DistcpBaseService.class);
  private final int numOfDirPerDistcpPerStream;
  protected final Path jarsPath;
  protected final Path auditUtilJarDestPath;

  public DistcpBaseService(DatabusConfig config, String name,
      Cluster srcCluster, Cluster destCluster, Cluster currentCluster,
      CheckpointProvider provider, Set<String> streamsToProcess)
          throws Exception {
    super(name + "_" + srcCluster.getName() + "_" + destCluster.getName(),
        config, streamsToProcess);
    this.srcCluster = srcCluster;
    this.destCluster = destCluster;
    if (currentCluster != null)
      this.currentCluster = currentCluster;
    else
      this.currentCluster = destCluster;
    //always return the HDFS read for the src cluster
    srcFs = FileSystem.get(new URI(srcCluster.getReadUrl()),
        srcCluster.getHadoopConf());
    destFs = FileSystem.get(new URI(destCluster.getHdfsUrl()),
        destCluster.getHadoopConf());
    Path tmpPath = new Path(destCluster.getTmpPath(), getName());
    this.tmpCounterOutputPath = new Path(tmpPath, "counters");
    this.provider = provider;
    String tmp;
    if ((tmp = System.getProperty(DatabusConstants.DIR_PER_DISTCP_PER_STREAM)) != null) {
      numOfDirPerDistcpPerStream = Integer.parseInt(tmp);
    } else
      numOfDirPerDistcpPerStream = DEFAULT_NUM_DIR_PER_DISTCP_STREAM;

    jarsPath = new Path(destCluster.getTmpPath(), "jars");
    auditUtilJarDestPath = new Path(jarsPath, "messaging-client-core.jar");
  }

  protected Cluster getSrcCluster() {
    return srcCluster;
  }

  protected Cluster getDestCluster() {
    return destCluster;
  }

  protected FileSystem getSrcFs() {
    return srcFs;
  }

  protected FileSystem getDestFs() {
    return destFs;
  }

  protected Boolean executeDistCp(String serviceName, 
      Map<String, FileStatus> fileListingMap, Path targetPath)
          throws Exception {
    //Add Additional Default arguments to the array below which gets merged
    //with the arguments as sent in by the Derived Service
    Configuration conf = currentCluster.getHadoopConf();
    conf.set(DatabusConstants.AUDIT_ENABLED_KEY,
        System.getProperty(DatabusConstants.AUDIT_ENABLED_KEY));
    conf.set("mapred.job.name", serviceName);
    conf.set("tmpjars", auditUtilJarDestPath.toString());
    // The first argument 'sourceFileListing' to DistCpOptions is not needed now 
    // since DatabusDistCp writes listing file using fileListingMap instead of
    // relying on sourceFileListing path. Passing a dummy value.
    DistCpOptions options = new DistCpOptions(new Path("/tmp"), targetPath);
    options.setOutPutDirectory(tmpCounterOutputPath);
    DistCp distCp = new DatabusDistCp(conf, options, fileListingMap);
    try {
      distCp.execute();
    } catch (Exception e) {
      LOG.error("Exception encountered ", e);
      throw e;
    }
    return true;
  }

  /*
   * @return remote Path from where this consumer can consume eg:
   * MergedStreamConsumerService - Path eg:
   * hdfs://remoteCluster/databus/system/consumers/<consumerName> eg:
   * MirrorStreamConsumerService - Path eg:
   * hdfs://remoteCluster/databus/system/mirrors/<consumerName>
   */
  protected abstract Path getInputPath() throws IOException;

  /*
   * @return the target path where distcp will copy paths from source cluster 
   */
  protected abstract Path getDistCpTargetPath();


  @Override
  public long getMSecondsTillNextRun(long currentTime) {
    long runIntervalInSec = (DEFAULT_RUN_INTERVAL / 1000);
    Calendar calendar = new GregorianCalendar();
    calendar.setTime(new Date(currentTime));
    long currentSec = calendar.get(Calendar.SECOND);
    return (runIntervalInSec - currentSec) * 1000;
  }


  protected abstract Path getStartingDirectory(String stream,
      List<FileStatus> filesToBeCopied) throws IOException;

  /*
   * Return a map of destination path,source path file status Since the map uses
   * destination path as the key,no conflicting duplicates paths would be
   * passed on to distcp
   * 
   * @return
   */
  protected Map<String, FileStatus> getDistCPInputFile()
      throws Exception {
    Map<String,FileStatus> result = new HashMap<String, FileStatus>();
    for (String stream : streamsToProcess) {
      int pathsAlreadyAdded = 0;
      LOG.info("Processing stream " + stream);
      byte[] value = provider.read(getCheckPointKey(stream));
      Path inputPath = new Path(getInputPath(), stream);
      Path lastCheckPointPath = null;
      Path nextPath = null;
      List<FileStatus> filesLastCopiedDir;
      if (value != null) {
        String checkPointValue = new String(value);
        // creating a path object from empty string throws exception;hence
        // checking for it
        if (!checkPointValue.trim().equals("")) {
          lastCheckPointPath = new Path(checkPointValue);
        }
        lastCheckPointPath = fullyQualifyCheckPointWithReadURL
            (lastCheckPointPath, srcCluster);
        if (lastCheckPointPath == null
            || !getSrcFs().exists(lastCheckPointPath)) {
          LOG.warn("Invalid checkpoint found [" + lastCheckPointPath
              + "] for stream " + stream + ";Ignoring it");
        } else {
          Date lastDate = CalendarHelper.getDateFromStreamDir(inputPath,
              lastCheckPointPath);
          nextPath = CalendarHelper.getNextMinutePathFromDate(lastDate,
              inputPath);
        }

      }
      if (nextPath == null) {
        filesLastCopiedDir = new ArrayList<FileStatus>();
        LOG.info("Finding the starting directoryfor stream [" + stream + "]");
        nextPath = getStartingDirectory(stream, filesLastCopiedDir);
        if (nextPath == null) {
          LOG.debug("No start directory found,returning the empty result");
          continue;
        }
        LOG.debug("Uncopied Files from directory last copied are "
            + FileUtil.toStringOfFileStatus(filesLastCopiedDir));
        for (FileStatus fileStatus : filesLastCopiedDir) {
          String destnPath = getFinalDestinationPath(fileStatus);
          if (destnPath != null) {
            LOG.info("Adding to input of Distcp.Move [" + fileStatus.getPath()
                + "] to " + destnPath);
            result.put(destnPath, fileStatus);
          }
        }
      }
      LOG.info("Starting directory for stream [" + stream + "]" + " is ["
          + nextPath + "]");
      Date nextDate = CalendarHelper.getDateFromStreamDir(inputPath, nextPath);
      // if next to next path exist than only add the next path so that the path
      // being added to disctp input is not the current path
      Path nextToNextPath = CalendarHelper.getNextMinutePathFromDate(nextDate,
          inputPath);
      Path lastPathAdded = null;
      FileStatus[] nextPathFileStatus;
      while (pathsAlreadyAdded <= numOfDirPerDistcpPerStream
          && srcFs.exists(nextToNextPath)
          && (nextPathFileStatus = FileUtil
          .listStatusAsPerHDFS(srcFs, nextPath)) != null) {
        if(nextPathFileStatus.length==0){
          LOG.info(nextPath + " is an empty directory");
          FileStatus srcFileStatus = srcFs.getFileStatus(nextPath); 
          String destnPath = getFinalDestinationPath(srcFileStatus);
          if(destnPath!=null){
            LOG.info("Adding to input of Distcp.Move ["+nextPath+"] to "+destnPath);
            result.put(destnPath,srcFileStatus);
          }
        }
        else{
          for(FileStatus fStatus:nextPathFileStatus){
            String destnPath = getFinalDestinationPath(fStatus);
            if(destnPath!=null){
              LOG.info("Adding to input of Distcp.Move ["+fStatus.getPath()+"] to "+destnPath);
              result.put(destnPath,fStatus);
            }
          }
        } 
        pathsAlreadyAdded++;
        lastPathAdded = nextPath;
        nextPath = nextToNextPath;
        nextDate = CalendarHelper.addAMinute(nextDate);
        nextToNextPath = CalendarHelper.getNextMinutePathFromDate(nextDate,
            inputPath);
      }
      if (lastPathAdded != null) {
        checkPointPaths.put(stream, lastPathAdded);
      }

    }
    return result;
  }

  /**
   * Method to qualify the checkpoint path based on the readurl configured
   * for the source cluster. The readurl of the cluster can change and the
   * checkpoint paths should be re-qualified to the new source cluster read
   * path.
   *
   * @param lastCheckPointPath path which can be null read from checkpoint
   *                           file.
   * @param srcCluster the cluster for which checkpoint file which should be
   *                   re-qualified.
   * @return path which is re-qualified.
   */
  protected Path fullyQualifyCheckPointWithReadURL(Path lastCheckPointPath,
      Cluster srcCluster) {
    //if checkpoint value was empty or null just fall thro' let the service
    // determine the new path.
    if(lastCheckPointPath == null) {
      return null;
    }
    String readUrl = srcCluster.getReadUrl();
    URI checkpointURI = lastCheckPointPath.toUri();
    String unQualifiedPathStr = checkpointURI.getPath();
    Path newCheckPointPath = new Path(readUrl,unQualifiedPathStr);
    return newCheckPointPath;
  }

  protected abstract String getFinalDestinationPath(FileStatus srcPath);

  protected String getCheckPointKey(String stream) {
    return getCheckPointKey(getClass().getSimpleName(), stream,
        srcCluster.getName());
  }

  protected void finalizeCheckPoints() throws Exception {
    for (Entry<String, Path> entry : checkPointPaths.entrySet()) {
      retriableCheckPoint(provider, getCheckPointKey(entry.getKey()), entry
          .getValue().toString().getBytes(), entry.getKey());
    }
    checkPointPaths.clear();
  }

  public Cluster getCurrentCluster() {
    // for tests
    return currentCluster;
  }

  public static void createListing(FileSystem fs, FileStatus fileStatus,
      List<FileStatus> results) throws IOException {
    if (fileStatus.isDir()) {
      FileStatus[] stats = FileUtil.listStatusAsPerHDFS(fs,
          fileStatus.getPath());
      // stats can be null in case where purger deleted the path while this
      // method was called
      if (stats != null) {
        if (stats.length == 0) {
          results.add(fileStatus);
          LOG.debug("createListing :: Adding [" + fileStatus.getPath() + "]");
        }
        for (FileStatus stat : stats) {
          createListing(fs, stat, results);
        }
      }
    } else {
      LOG.debug("createListing :: Adding [" + fileStatus.getPath() + "]");
      results.add(fileStatus);
    }
  }

  /*
   * Find the topic name from path of format
   * /databus/streams/<streamName>/2013/10/01/09/17 or
   * /databus/streams/<streamName>/2013/10/
   * 01/09/17/<collectorName>-<streamName>-2013-10-01-09-13_00000.gz
   */
  protected String getTopicNameFromDestnPath(Path destnPath) {
    String destnPathAsString =destnPath.toString();
    String destnDirAsString =new Path(destCluster.getFinalDestDirRoot()).toString();
    String pathWithoutRoot = destnPathAsString.substring(destnDirAsString
        .length());
    Path tmpPath = new Path(pathWithoutRoot);
    while (tmpPath.depth() != 1)
      tmpPath=tmpPath.getParent();
    return tmpPath.getName();
  }
}
