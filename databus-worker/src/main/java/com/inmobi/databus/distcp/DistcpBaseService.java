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

import java.io.IOException;
import java.net.URI;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpConstants;
import org.apache.hadoop.tools.DistCpOptions;

import com.inmobi.databus.AbstractService;
import com.inmobi.databus.CheckpointProvider;
import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.utils.CalendarHelper;

public abstract class DistcpBaseService extends AbstractService {

  protected final Cluster srcCluster;
  protected final Cluster destCluster;
  protected final Cluster currentCluster;
  private final FileSystem srcFs;
  private final FileSystem destFs;
  protected static final int DISTCP_SUCCESS = DistCpConstants.SUCCESS;
  protected final CheckpointProvider provider;
  protected Map<String, Path> checkPointPaths = new HashMap<String, Path>();

  protected static final Log LOG = LogFactory.getLog(DistcpBaseService.class);

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
    srcFs = FileSystem.get(new URI(srcCluster.getHdfsUrl()),
        srcCluster.getHadoopConf());
    destFs = FileSystem.get(new URI(destCluster.getHdfsUrl()),
        destCluster.getHadoopConf());
    this.provider = provider;
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
    conf.set("mapred.job.name", serviceName + "_" + getSrcCluster().getName() +
        "_" + getDestCluster().getName());
    
    // The first argument 'sourceFileListing' to DistCpOptions is not needed now 
    // since DatabusDistCp writes listing file using fileListingMap instead of
    // relying on sourceFileListing path. Passing a dummy value.
    DistCpOptions options = new DistCpOptions(new Path("/tmp"), targetPath);
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
   * @param filename should be of the format
   * collectorname-streamname-yyyy-mm-dd-hh-mn_xxxxx.gz where xxxxx is the index
   * of the file
   * 
   * @param streamSet set of all the destination streams of the corresponding
   * cluster
   * 
   * @return null if the filename is not of correct format or no categories in
   * the streamset matches with the category in the filename
   */
  protected static String getCategoryFromFileName(String fileName,
      Set<String> streamsSet) {
    for (String streamName : streamsSet) {
      String strs[] = fileName.split(streamName);
      if (strs.length == 2) {
        if (checkCorrectDateFormat(strs[1]))
          return streamName;
      }
    }
    return null;
  }

  protected static boolean checkCorrectDateFormat(String timestamp) {
    return timestamp
        .matches("^-[0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{2}_[0-9]{5}.gz$");
  }

  @Override
  public long getMSecondsTillNextRun(long currentTime) {
    long runIntervalInSec = (DEFAULT_RUN_INTERVAL / 1000);
    Calendar calendar = new GregorianCalendar();
    calendar.setTime(new Date(currentTime));
    long currentSec = calendar.get(Calendar.SECOND);
    return (runIntervalInSec - currentSec) * 1000;
  }


  protected abstract Path getStartingDirectory(String stream) throws IOException;

  /*
   * Return a map of destination path,source path file status Since the map uses
   * destination path as the key,no conflicting duplicates paths would be passed
   * on to distcp
   * 
   * @return
   */
  protected Map<String, FileStatus> getDistCPInputFile()
      throws Exception {
    Map<String,FileStatus> result = new HashMap<String, FileStatus>();
    for (String stream : streamsToProcess) {
      byte[] value = provider.read(getCheckPointKey(stream));
      Path inputPath = new Path(getInputPath(), stream);
      Path lastCheckPointPath = null;
      Path nextPath = null;
      if (value != null) {
        String checkPointValue = new String(value);
        // creating a path object from empty string throws exception;hence
        // checking for it
        if (checkPointValue.trim() != null) {
        lastCheckPointPath = new Path(checkPointValue);
        }
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
        LOG.info("Finding the starting directoryfor stream [" + stream + "]");
        nextPath = getStartingDirectory(stream);
        if (nextPath == null) {
          LOG.debug("No start directory found,returning the empty result");
          return result;
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
      FileStatus[] nextPathFileStatus = listStatusAsPerHDFS(srcFs, nextPath);
      FileStatus[] nextToNextPathFileStatus;
      while ((nextToNextPathFileStatus = listStatusAsPerHDFS(srcFs,
          nextToNextPath)) != null) {
        if(nextPathFileStatus.length==0){
          LOG.info(nextPath + " is an empty directory");
          FileStatus srcFileStatus = srcFs.getFileStatus(nextPath); 
          String destnPath= getFinalDestinationPath(srcFileStatus);
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
        lastPathAdded = nextPath;
        nextPath = nextToNextPath;
        nextDate = CalendarHelper.addAMinute(nextDate);
        nextToNextPath = CalendarHelper.getNextMinutePathFromDate(nextDate,
            inputPath);
        nextPathFileStatus=nextToNextPathFileStatus;
        nextToNextPathFileStatus=null;
      }
      if (lastPathAdded != null) {
        checkPointPaths.put(stream, lastPathAdded);
      }

    }
    return result;
  }

  /*
   * FileSystem.listStatus behaves differently for different filesystem This
   * helper method would ensure a consistent behaviour which is 1) If the path
   * is a dir and has data return list of filestatus 2) If the path is a dir but
   * is empty return empty list 3) If the path doesn't exist return null
   * Different behaviors by different filesystem are HDFS S3N Local Dir With
   * Data Array Array Array Empty Directory Empty Array null Empty Array Non
   * Existent Path null null Empty Array
   */
  private FileStatus[] listStatusAsPerHDFS(FileSystem fs, Path p)
      throws IOException {
    FileStatus[] fStatus = fs.listStatus(p);
    if (fStatus != null && fStatus.length > 0)
      return fStatus;
    if (fs.exists(p))
      return new FileStatus[0];
    return null;

  }
  protected abstract String getFinalDestinationPath(FileStatus srcPath);

  protected String getCheckPointKey(String stream) {
    return getClass().getSimpleName() + srcCluster.getName() + stream;
  }

  // protected abstract void filterMinFilePaths(Set<String> minFilesSet);

  protected void finalizeCheckPoints() {
    for (Entry<String, Path> entry : checkPointPaths.entrySet()) {
      provider.checkpoint(getCheckPointKey(entry.getKey()), entry.getValue()
          .toString().getBytes());
    }
  }

  public Cluster getCurrentCluster() {
    // for tests
    return currentCluster;
  }

  public static void createListing(FileSystem fs, FileStatus fileStatus,
      List<FileStatus> results) throws IOException {
    if (fileStatus.isDir()) {
      FileStatus[] stats = fs.listStatus(fileStatus.getPath());
      if (stats.length == 0) {
        results.add(fileStatus);
        LOG.debug("createListing :: Adding [" + fileStatus.getPath() + "]");
      }
      for (FileStatus stat : stats) {
        createListing(fs, stat, results);
      }
    } else {
      LOG.debug("createListing :: Adding [" + fileStatus.getPath() + "]");
      results.add(fileStatus);
    }
  }
}
