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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpConstants;
import org.apache.hadoop.tools.DistCpOptions;

import com.inmobi.databus.AbstractService;
import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.utils.CalendarHelper;

public abstract class DistcpBaseService extends AbstractService {

  private final Cluster srcCluster;
  private final Cluster destCluster;
  private final FileSystem srcFs;
  private final FileSystem destFs;
  protected static final int DISTCP_SUCCESS = DistCpConstants.SUCCESS;

  protected static final Log LOG = LogFactory.getLog(DistcpBaseService
      .class);

  public DistcpBaseService(DatabusConfig config, String name,
                           Cluster srcCluster,
                           Cluster destCluster) throws Exception {
    super(name + "_" +
        srcCluster.getName() + "_" + destCluster.getName(), config);
    this.srcCluster = srcCluster;
    this.destCluster = destCluster;
    srcFs = FileSystem.get(new URI(srcCluster.getHdfsUrl()),
        srcCluster.getHadoopConf());
    destFs = FileSystem.get(
        new URI(destCluster.getHdfsUrl()), destCluster.getHadoopConf());
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
    // If more command line options need to be passed to DistCP then, 
    // Create options instance using OptionParser.parse and set default options
    // on the returned instance.
    //with the arguments as sent in by the Derived Service
    return options;
  }

  protected Boolean executeDistCp(DistCpOptions options)
      throws Exception
  {
    //Add Additional Default arguments to the array below which gets merged
    //with the arguments as sent in by the Derived Service
    Configuration conf = destCluster.getHadoopConf();
    DistCp distCp = new DistCp(conf, options);
    try {
      distCp.execute();
    } catch (Exception e) {
      LOG.error("Exception encountered ", e);
      throw e;
    }
    return true;
  }

  /*
  * @return remote Path from where this consumer can consume
  * eg: MergedStreamConsumerService - Path eg:
  * hdfs://remoteCluster/databus/system/consumers/<consumerName>
  * eg: MirrorStreamConsumerService - Path eg:
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
    long runIntervalInSec = (DEFAULT_RUN_INTERVAL/1000);
    Calendar calendar = new GregorianCalendar();
    calendar.setTime(new Date(currentTime));
    long currentSec = calendar.get(Calendar.SECOND);
    return (runIntervalInSec - currentSec) * 1000;
  }

  protected void doFinalCommit(Map<Path, FileSystem> consumePaths) throws
      Exception {
    //commit distcp consume Path from remote cluster
    Set<Map.Entry<Path, FileSystem>> consumeEntries = consumePaths.entrySet();
    for (Map.Entry<Path, FileSystem> consumePathEntry : consumeEntries) {
      FileSystem fileSystem = consumePathEntry.getValue();
      Path consumePath = consumePathEntry.getKey();
      fileSystem.delete(consumePath);
      LOG.debug("Deleting/Commiting [" + consumePath + "]");
    }

  }


  /*
   * @param Map<Path, FileSystem> consumePaths - list of files which contain
   * fully qualified path of minute level files which have to be pulled
   * @param Path tmp - Temporary Location path on Cluster to where files have
   * to be pulled
   *
   * @return
   */
  protected Path getDistCPInputFile(Map<Path, FileSystem> consumePaths,
                                    Path tmp) throws Exception {
    Path input = getInputPath();
    if (!srcFs.exists(input))
      return null;
    //find all consumePaths which need to be pulled
    FileStatus[] fileList = srcFs.listStatus(input);
    if (fileList != null) {
      Set<String> minFilesSet = new HashSet<String>();
      Set<Path> yetToBeMovedPaths = new HashSet<Path>();
      /*
       * inputPath could have multiple files due to backlog read all and create
       * a Input file for DISTCP with valid paths on destinationCluster as an
       * optimization so that distcp doesn't pull this file remotely
       */
      for (int i = 0; i < fileList.length; i++) {
        Path consumeFilePath = fileList[i].getPath().makeQualified(srcFs);
        /* put eachFile name in consumePaths
        * An example of data in consumePaths is
        * /databus/system/consumers/<cluster>/file1..and so on
        */
        consumePaths.put(consumeFilePath, srcFs);

        LOG.debug("Reading minutePaths from ConsumePath [" +
            consumeFilePath + "]");
        //read all valid minute files path in each consumePath
        readConsumePath(srcFs, consumeFilePath,
            minFilesSet, yetToBeMovedPaths);
      }
      // removing those path which have same file name
      filterMinFilePaths(minFilesSet);
      // writing yet to be moved file on the source cluster
      writeYetToBeMovedFile(srcCluster.getTmpPath(), yetToBeMovedPaths);

      Path tmpPath = createInputFileForDISCTP(destFs, srcCluster.getName(),
          tmp, minFilesSet);
      return getFinalPathForDistCP(tmpPath, consumePaths);
    }

    return null;
  }

  protected abstract void filterMinFilePaths(Set<String> minFilesSet);

  /*
   * read each consumePath and add only valid paths to minFilesSet
   */
  void readConsumePath(FileSystem fs, Path consumePath,
      Set<String> minFilesSet, Set<Path> yetToBeMovedPaths) throws IOException {
    BufferedReader reader = null;
    try {
      FSDataInputStream fsDataInputStream = fs.open(consumePath);
      reader = new BufferedReader(new InputStreamReader
          (fsDataInputStream));
      String minFileName = null;
      do {
        minFileName = reader.readLine();
        if (minFileName != null) {
          /*
          * To avoid data-loss in all services we publish the paths to
          * consumers directory first before publishing on HDFS for
          * finalConsumption. In a distributed transaction failure it's
          * possible that some of these paths do not exist. Do isExistence
          * check before adding them as DISTCP input otherwise DISTCP
          * jobs can fail continously thereby blocking Merge/Mirror
          * stream to run further
          */
          Path p = new Path(minFileName);
          if (fs.exists(p)) {
            LOG.info("Adding sourceFile [" + minFileName + "] to distcp " +
                "FinalList");
            minFilesSet.add(minFileName.trim());
          } else {
            // if a path doesn't exist than it there can be 2 cases:
            // 1) previous service failed after writing consumer paths
            // 2) previous service is still working and its in between writing
            // consumer paths and publishing data
            if (isMoveFailed(fs, p)) {
              LOG.info("Skipping [" + minFileName + "] to pull as it's an "
                  + "INVALID PATH");
            } else {
              yetToBeMovedPaths.add(p);
            }
          }
        }
      } while (minFileName != null);
    } finally {
      if (reader != null)
        reader.close();
    }
  }

  /*
   * Method to check whether movement of file from temp location to Path p has
   * failed or is under process
   */
  private boolean isMoveFailed(FileSystem fs, Path p) throws IOException {
    Path nextPath = getNextMinuteDirectory(p);
    if (nextPath == null) {
      LOG.error("Path[" + p + "] is invalid");
      return true;// so that invalid path can be skipped
    }
    if (fs.exists(nextPath))
      return true;
    return false;
  }

  /*
   * Helper function to find next minute directory given a path eg: Path p =
   * "hdfs://ruby-nn.grid.ua2.inmobi.com:8020/databus/streams/adroit_feedback_delta/2013/01/10/12/33"
   */
  private Path getNextMinuteDirectory(Path p) {
    String rootString = srcCluster.getRootDir();
    Path root = new Path(rootString);// this will clean the root path string of
                                     // all extra slashes(/)
    // eg: root = "hdfs://ruby-nn.grid.ua2.inmobi.com:8020/databus/"
    LOG.debug("Path given to getNextMinuteDirectory function [" + p + "]");
    String path = p.toString();
    // if it is a valid path than this condition should be true
    if (path.length() >= root.toString().length() + 1) {
      // tmpPath will have value of form
      // "streams/adroit_feedback_delta/2013/01/10/12/33"
      String tmpPath = path.substring(root.toString().length() + 1);
      String tmp[] = tmpPath.split(File.separator);
      if (tmp.length >= 2) {
        // tmp[0] value should either be streams or steam_local and tmp[1] value
        // will be name of topic
        Path pathTillStream = new Path(root, tmp[0]);
        Path pathTillTopic = new Path(pathTillStream, tmp[1]);
        // pathTillTopic value will be of form
        // hdfs://ruby-nn.grid.ua2.inmobi.com:8020/databus/streams/adroit_feedback_delta/
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

  void writeYetToBeMovedFile(Path tmp, Set<Path> yetToBeMovedPaths)
      throws Exception {
    String tmpPath = "yet_to_be_moved_src" + srcCluster.getName()
        + "_to_destn_" + destCluster.getName() + "_"
        + System.currentTimeMillis();
    Path temporary = new Path(tmp, tmpPath);
    Map<Path, Path> ytmCommitPaths = new HashMap<Path, Path>();
    FSDataOutputStream out = null;
    try {
      out = srcFs.create(temporary);
      for (Path p : yetToBeMovedPaths) {
        LOG.debug("Writing yet To Be Moved Path [" + p + "]");
        out.writeBytes(p.toString());
        out.writeBytes("\n");
      }
    } catch (IOException e) {
      LOG.error("Cannot create yet_to_be_moved file in[" + temporary + "]", e);
    } finally {
      if (out != null) {
        try {
          out.close();
        } catch (IOException e) {
          LOG.error("Cannot close the file [" + temporary + "]", e);
        }
      }
    }

    Path finalYetToBeMoved = new Path(getInputPath(),
        ("yet_to_be_moved_" + System.currentTimeMillis()));
    ytmCommitPaths.put(temporary, finalYetToBeMoved);
    LOG.info("Renaming " + temporary + " to " + finalYetToBeMoved);
    srcFs.mkdirs(finalYetToBeMoved.getParent());
    if (srcFs.rename(temporary, finalYetToBeMoved) == false) {
      LOG.warn("Rename failed, aborting transaction COMMIT to avoid "
          + "dataloss.");
      throw new Exception("Abort transaction Commit. Rename failed from ["
          + temporary + "] to [" + finalYetToBeMoved + "]");
    }
  }

  /*
   * Helper function which returns the final path to be used as input forDistcp
   * Does cleanup of consumePaths at sourceCluster if they are INVALID
   */
  private Path getFinalPathForDistCP(Path tmpPath, Map<Path,
      FileSystem> consumePaths)throws IOException{
    if (tmpPath != null) {
      LOG.warn("Source File For distCP [" + tmpPath + "]");
      consumePaths.put(tmpPath.makeQualified(destFs), destFs);
      return tmpPath.makeQualified(destFs);
    } else {
      /*
      * no valid paths to return.
      */
      return null;
    }
  }


  /*
   * Helper method for getDistCPInputFile
   * if none of the paths are VALID then it does not create an empty file on
   * <clusterName> but returns a null
   * @param FileSystem - where to create file i.e. srcFs or destFs
   * @param String - sourceCluster from where we are pulling files from
   * @param Path - tmpLocation on sourceCluster
   * @param Set<String> - set of sourceFiles need to be pulled
   */
  private Path createInputFileForDISCTP(FileSystem fs, String clusterName,
                                        Path tmp, Set<String> minFilesSet)
      throws IOException {
    if (minFilesSet.size() > 0) {
      Path tmpPath = null;
      FSDataOutputStream out = null;
      try {
        tmpPath = new Path(tmp, clusterName + new Long(System
            .currentTimeMillis()).toString());
        out = fs.create(tmpPath);
        for (String minFile : minFilesSet) {
          out.write(minFile.getBytes());
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


}
