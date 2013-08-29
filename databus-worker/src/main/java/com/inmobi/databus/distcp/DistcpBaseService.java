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
import com.inmobi.databus.DatabusConstants;
import com.inmobi.databus.utils.CalendarHelper;
import com.inmobi.databus.utils.FileUtil;

public abstract class DistcpBaseService extends AbstractService {

  protected final Cluster srcCluster;
  protected final Cluster destCluster;
  protected final Cluster currentCluster;
  private final FileSystem srcFs;
  private final FileSystem destFs;
  protected static final int DISTCP_SUCCESS = DistCpConstants.SUCCESS;
  protected final CheckpointProvider provider;
  protected Map<String, Path> checkPointPaths = new HashMap<String, Path>();
  private static final int DEFAULT_NUM_DIR_PER_DISTCP = 30;

  protected static final Log LOG = LogFactory.getLog(DistcpBaseService.class);
  private final int numOfDirPerDistcp;

  public DistcpBaseService(DatabusConfig config, String name,
      Cluster srcCluster, Cluster destCluster, Cluster currentCluster,
      CheckpointProvider provider, Set<String> streamsToProcess)
      throws Exception {
    super(name + "_" + srcCluster.getName() + "_" + destCluster.getName(),
        config,streamsToProcess);
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
    String tmp;
    if ((tmp = System.getProperty(DatabusConstants.NUM_DIR_PER_DISTCP)) != null) {
      numOfDirPerDistcp = Integer.parseInt(tmp);
    } else
      numOfDirPerDistcp = DEFAULT_NUM_DIR_PER_DISTCP;
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

  /**
   * Set Common or default DistCp options here.
   * 
   * @param inputPathListing
   * @param target
   * @return options instance
   */

  protected DistCpOptions getDistCpOptions(Path inputPathListing, Path target) {
    DistCpOptions options = new DistCpOptions(inputPathListing, target);
    options.setBlocking(true);
    options.setSkipPathValidation(true);
    options.setUseSimpleFileListing(true);
    // If more command line options need to be passed to DistCP then,
    // Create options instance using OptionParser.parse and set default options
    // on the returned instance.
    // with the arguments as sent in by the Derived Service
    return options;
  }

  protected Boolean executeDistCp(String serviceName,
      Map<String, FileStatus> fileListingMap, Path targetPath)
      throws Exception {
    //Add Additional Default arguments to the array below which gets merged
    //with the arguments as sent in by the Derived Service
    Configuration conf = currentCluster.getHadoopConf();
    conf.set("mapred.job.name", serviceName);

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
   * @return the target path where distcp will copy paths from source cluster
   */
  protected abstract Path getDistCpTargetPath();

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

  protected void doFinalCommit(Map<Path, FileSystem> consumePaths)
      throws Exception {
    // commit distcp consume Path from remote cluster
    Set<Map.Entry<Path, FileSystem>> consumeEntries = consumePaths.entrySet();
    for (Map.Entry<Path, FileSystem> consumePathEntry : consumeEntries) {
      FileSystem fileSystem = consumePathEntry.getValue();
      Path consumePath = consumePathEntry.getKey();
      if (retriableDelete(fileSystem, consumePath)) {
      LOG.debug("Deleting/Commiting [" + consumePath + "]");
      } else {
        LOG.error("Deleting [" + consumePath + "] failed,Data replay possible");
      }
    }
  }


  protected abstract Path getStartingDirectory(String stream,
      List<FileStatus> filesToBeCopied) throws IOException;

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
      int pathsAlreadyAdded = 0;
      for (String stream : streamsToProcess) {
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
        FileStatus[] nextPathFileStatus = FileUtil.listStatusAsPerHDFS(srcFs, nextPath);
        FileStatus[] nextToNextPathFileStatus;
        while (pathsAlreadyAdded <= numOfDirPerDistcp
            && nextPathFileStatus != null
            && (nextToNextPathFileStatus = FileUtil.listStatusAsPerHDFS(srcFs,
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
          pathsAlreadyAdded++;
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
   * Helper function to find next minute directory given a path eg: Path p =
   * "hdfsUrl/databus/streams/<streamName>/2013/01/10/12/33"
   */
  private Path getNextMinuteDirectory(Path p) {
    String rootString = srcCluster.getRootDir();
    Path root = new Path(rootString);// this will clean the root path string of
                                     // all extra slashes(/)
    // eg: root = "hdfsUrl/databus/"
    LOG.debug("Path given to getNextMinuteDirectory function [" + p + "]");
    String path = p.toString();
    // if it is a valid path than this condition should be true
    if (path.length() >= root.toString().length() + 1) {
      // tmpPath will have value of form
      // "streams/<streamName>/2013/01/10/12/33"
      String tmpPath = path.substring(root.toString().length() + 1);
      String tmp[] = tmpPath.split(File.separator);
      if (tmp.length >= 2) {
        // tmp[0] value should either be streams or steam_local and tmp[1] value
        // will be name of topic
        Path pathTillStream = new Path(root, tmp[0]);
        Path pathTillTopic = new Path(pathTillStream, tmp[1]);
        // pathTillTopic value will be of form
        // hdfsUrl/databus/streams/<streamName>/
        Date pathDate = CalendarHelper.getDateFromStreamDir(pathTillTopic, p);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(pathDate);
        calendar.add(Calendar.MINUTE, 1);
        Date nextdate = calendar.getTime();
        String suffix = Cluster.getDateAsYYYYMMDDHHMNPath(nextdate);
        Path nextPath = new Path(pathTillTopic, suffix);
        return nextPath;
      }
    }

    return null;
  }

  protected abstract String getFinalDestinationPath(FileStatus srcPath);

  protected String getCheckPointKey(String stream) {
    return getCheckPointKey(getClass().getSimpleName(), stream,
        srcCluster.getName());
  }

  protected void finalizeCheckPoints() {
    for (Entry<String, Path> entry : checkPointPaths.entrySet()) {
      provider.checkpoint(getCheckPointKey(entry.getKey()), entry.getValue()
          .toString().getBytes());
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
      FileStatus[] stats = fs.listStatus(fileStatus.getPath());
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
}
