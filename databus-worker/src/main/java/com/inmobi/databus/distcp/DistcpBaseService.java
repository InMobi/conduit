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
import org.apache.hadoop.fs.FSDataOutputStream;
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
    
    DistCpOptions options = new DistCpOptions(new Path(""), targetPath);
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

  protected void doFinalCommit(Map<Path, FileSystem> consumePaths)
      throws Exception {
    // commit distcp consume Path from remote cluster
    Set<Map.Entry<Path, FileSystem>> consumeEntries = consumePaths.entrySet();
    for (Map.Entry<Path, FileSystem> consumePathEntry : consumeEntries) {
      FileSystem fileSystem = consumePathEntry.getValue();
      Path consumePath = consumePathEntry.getKey();
      fileSystem.delete(consumePath);
      LOG.debug("Deleting/Commiting [" + consumePath + "]");
    }

  }

  protected abstract byte[] createCheckPoint(String stream) throws IOException;

  /*
   * Return a map of destination path,source path file status
   * Since the map uses destination path as the key,no conflicting duplicates 
   * paths woule be passed on to distcp
   * 
   * @return
   */
  protected Map<String, FileStatus> getDistCPInputFile()
      throws Exception {
    String checkPointValue = null;
    Map<String,FileStatus> result = new HashMap<String, FileStatus>();
    for (String stream : streamsToProcess) {
      byte[] value = provider.read(getCheckPointKey(stream));
      if (value != null)
        checkPointValue = new String(value);
      else {
        LOG.info("No checkpoint found for stream [" + stream + "]");
        checkPointValue = new String(createCheckPoint(stream));
        LOG.info("CheckPoint value created for stream [" + stream + "] is "
            + checkPointValue);
      }
      Path lastProcessed = new Path(checkPointValue);
      LOG.info("Last processed path for stream [" + stream + "]" + " is ["
          + lastProcessed + "]");
      Path inputPath = new Path(getInputPath(), stream);
      Date lastDate = CalendarHelper.getDateFromStreamDir(inputPath,
          lastProcessed);
      LOG.info("Data processed till [" + lastDate + "] for stream " + stream);
      Path nextPath = CalendarHelper.getNextMinutePathFromDate(lastDate,
          inputPath);
      Date nextDate = CalendarHelper.addAMinute(lastDate);
      // if next to next path exist than only add the next path so that the path
      // being added to disctp input is not the current path
      Path nextToNextPath = CalendarHelper.getNextMinutePathFromDate(nextDate,
          inputPath);
      Path lastPathAdded = null;
      FileStatus[] nextPathFileStatus=srcFs.listStatus(nextPath);
      FileStatus[] nextToNextPathFileStatus;
      while ((nextToNextPathFileStatus=srcFs.listStatus(nextToNextPath))!=null) {
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




  /*
   * Helper method for getDistCPInputFile if none of the paths are VALID then it
   * does not create an empty file on <clusterName> but returns a null
   * 
   * @param FileSystem - where to create file i.e. srcFs or destFs
   * 
   * @param String - sourceCluster from where we are pulling files from
   * 
   * @param Path - tmpLocation on sourceCluster
   * 
   * @param Set<String> - set of sourceFiles need to be pulled
   */
  private Path createInputFileForDISCTP(FileSystem fs, String clusterName,
      Path tmp, Set<Path> minFilesSet) throws IOException {
    if (minFilesSet.size() > 0) {
      Path tmpPath = null;
      FSDataOutputStream out = null;
      try {
        tmpPath = new Path(tmp, clusterName
            + new Long(System.currentTimeMillis()).toString());
        out = fs.create(tmpPath);
        for (Path minFile : minFilesSet) {
          out.write(minFile.toString().getBytes());
          out.write('\n');
        }
      } finally {
        if (out != null) {
          out.close();
        }
      }
      return tmpPath;
    } else
      return null;
  }

  public Cluster getCurrentCluster() {
    // for tests
    return currentCluster;
  }

  void createListing(FileSystem fs, FileStatus fileStatus,
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
