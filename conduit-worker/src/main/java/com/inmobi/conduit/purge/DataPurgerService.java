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
package com.inmobi.conduit.purge;

import java.io.FileNotFoundException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.inmobi.conduit.utils.CalendarHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.inmobi.conduit.metrics.ConduitMetrics;
import com.inmobi.conduit.AbstractService;
import com.inmobi.conduit.Cluster;
import com.inmobi.conduit.ConduitConfig;
import com.inmobi.conduit.ConduitConfigParser;
import com.inmobi.conduit.DestinationStream;
import com.inmobi.conduit.SourceStream;

/*
 * Assumptions
 * (i) One data Purger Service for a cluster
 */

public class DataPurgerService extends AbstractService {
  private static final Log LOG = LogFactory.getLog(DataPurgerService.class);

  private final Cluster cluster;
  private final FileSystem fs;
  private final Integer defaulttrashPathRetentioninHours;
  private final Integer defaultstreamPathRetentioninHours;
  private Map<String, Integer> streamRetention;
  private Set<Path> streamsToPurge;
  private DateFormat dateFormat = new SimpleDateFormat("yyyy:MM:dd:HH:mm");
  private static long MILLISECONDS_PER_HOUR = 60 * 60 * 1000;
  private final static String PURGEPATHS_COUNT = "purgePaths.count";
  private final static String DELETE_FAILURES_COUNT = "deleteFailures.count";

  public DataPurgerService(ConduitConfig conduitConfig, Cluster cluster)
      throws Exception {
    super(DataPurgerService.class.getName(), conduitConfig, 60000 * 60, null,
        new HashSet<String>());
    this.cluster = cluster;
    fs = FileSystem.get(cluster.getHadoopConf());
    this.defaulttrashPathRetentioninHours = new Integer(
        Integer.parseInt(conduitConfig
            .getDefaults().get(ConduitConfigParser.TRASH_RETENTION_IN_HOURS)));
    this.defaultstreamPathRetentioninHours = new Integer(
        Integer.parseInt(conduitConfig.getDefaults().get(
            ConduitConfigParser.RETENTION_IN_HOURS)));
    ConduitMetrics.registerSlidingWindowGauge(getServiceType(),
        PURGEPATHS_COUNT, getName());
    ConduitMetrics.registerSlidingWindowGauge(getServiceType(),
        DELETE_FAILURES_COUNT, getName());
    ConduitMetrics.registerSlidingWindowGauge(getServiceType(), RUNTIME,
        getName());
    ConduitMetrics.registerSlidingWindowGauge(getServiceType(), FAILURES,
        getName());
  }

  @Override
  public void stop() {
    stopped = true;
    /*
     * Pruger can sleep for an hour so it needs to be interuppted
     */
    thread.interrupt();
    LOG.info(Thread.currentThread().getName() + " stopped [" + stopped + "]");
  }

  @Override
  public long getMSecondsTillNextRun(long currentTime) {
    return runIntervalInMsec;
  }

  private void addMergedStreams() {
    Map<String, DestinationStream> destinationStreamMapStreamMap = cluster
        .getDestinationStreams();
    Set<Map.Entry<String, DestinationStream>> entrySet = destinationStreamMapStreamMap
        .entrySet();
    Iterator it = entrySet.iterator();
    while (it.hasNext()) {
      Map.Entry entry = (Map.Entry) it.next();
      String streamName = (String) entry.getKey();
      DestinationStream consumeStream = (DestinationStream) entry.getValue();
      Integer mergedStreamRetentionInHours = consumeStream
          .getRetentionInHours();
      LOG.debug("Merged Stream :: streamName [" + streamName
          + "] mergedStreamRetentionInHours [" + mergedStreamRetentionInHours
          + "]");
      if (streamRetention.get(streamName) == null) {
        streamRetention.put(streamName, mergedStreamRetentionInHours);
        LOG.debug("Adding Merged Stream [" + streamName
            + "] retentionInHours [" + mergedStreamRetentionInHours + "]");
      } else {
        // Partial & Merged stream are produced at this cluster
        // choose max retention period
        Integer partialStreamRetentionInHours = streamRetention.get(streamName);
        if (partialStreamRetentionInHours
            .compareTo(mergedStreamRetentionInHours) > 0) {
          streamRetention.put(streamName, partialStreamRetentionInHours);
          LOG.debug("Overriding Stream [" + streamName + "] retentionInHours ["
              + partialStreamRetentionInHours + "]");

        } else {
          streamRetention.put(streamName, mergedStreamRetentionInHours);
          LOG.debug("Overriding Stream [" + streamName + "] retentionInHours ["
              + mergedStreamRetentionInHours + "]");

        }

      }
    }
  }

  private void addLocalStreams() {
    for (SourceStream s : getConfig().getSourceStreams().values()) {
      if (s.getSourceClusters().contains(cluster.getName())) {
        String streamName = s.getName();
        Integer retentionInHours = new Integer(s.getRetentionInHours(cluster
            .getName()));
        streamRetention.put(streamName, retentionInHours);
        LOG.debug("Adding Partial Stream [" + streamName
            + "] with retentionPeriod [" + retentionInHours + " Hours]");
      }
    }
  }

  private Integer getDefaultStreamRetentionInHours() {
    return defaultstreamPathRetentioninHours;
  }

  private Integer getTrashPathRetentionInHours() {
    return defaulttrashPathRetentioninHours;
  }

  Integer getRetentionPeriod(String streamName) {
    Integer retentionInHours = streamRetention.get(streamName);
    if (retentionInHours == null)
      return getDefaultStreamRetentionInHours();
    return retentionInHours;
  }

  @Override
  protected void execute() throws Exception {
    try {
      streamRetention = new HashMap<String, Integer>();
      streamsToPurge = new TreeSet<Path>();

      // populates - streamRetention
      // Map of streams and their retention period at this cluster (Partial +
      // Merged)
      // Partial streams produced at this cluster - retention period config
      addLocalStreams();
      // Merged streams at this cluster - retention period config
      addMergedStreams();
      String mergedStreamRoot = cluster.getFinalDestDirRoot();
      Map<String, Path> mergedStreamsInClusterPathMap = getStreamsInCluster(mergedStreamRoot);
      String localStreamRoot = cluster.getLocalFinalDestDirRoot();
      Map<String, Path> localStreamsInClusterPathMap = getStreamsInCluster(localStreamRoot);
      getPathsToPurge(mergedStreamsInClusterPathMap,
          localStreamsInClusterPathMap);
      purge();
    } catch (Exception e) {
      LOG.warn(e);
      e.printStackTrace();
      throw new Exception(e);
    }
  }

  private void getPathsToPurge(Map<String, Path> mergedStreamsInClusterPathMap,
      Map<String, Path> localStreamsInClusterPathMap) throws Exception {
    getStreamsPathToPurge(mergedStreamsInClusterPathMap);
    getStreamsPathToPurge(localStreamsInClusterPathMap);
    getTrashPathsToPurge();

  }

  private void getTrashPathsToPurge() throws Exception {
    Path trashRoot = cluster.getTrashPath();
    LOG.debug("Looking for trashPaths in [" + trashRoot + "]");
    FileStatus[] trashDatePaths = getAllFilesInDir(trashRoot, fs);
    // For each trashpath
    if (trashDatePaths != null && trashDatePaths.length >= 1) {
      for (FileStatus trashPath : trashDatePaths) {
        FileStatus[] trashHourPaths = getAllFilesInDir(trashPath.getPath(), fs);
        if (trashHourPaths != null && trashHourPaths.length >= 1) {
          for (FileStatus trashHourPath : trashHourPaths) {
            try {
              Calendar trashPathHourDate = getDateFromTrashPath(trashPath
                  .getPath().getName(), trashHourPath.getPath().getName());
              if (isPurge(trashPathHourDate, getTrashPathRetentionInHours()))
                streamsToPurge.add(trashHourPath.getPath().makeQualified(fs));
            } catch (NumberFormatException e) {
              streamsToPurge.add(trashHourPath.getPath().makeQualified(fs));
            }
          }
        } else {
          try {
            /*
             * The date direcotry is empty. Check the time elapsed between the
             * last hour of the empty directory (23rd hour) and the current time
             * and add the date direcotry to the streamsToPurge only if the time
             * elapsed is greater than the trashPathRetiontionHours
             */
            Calendar trashPathDate = getDateFromTrashPath(trashPath.getPath()
                .getName(), "23");
            if (isPurge(trashPathDate, getTrashPathRetentionInHours()))
              streamsToPurge.add(trashPath.getPath().makeQualified(fs));
          } catch (NumberFormatException e) {
            streamsToPurge.add(trashPath.getPath().makeQualified(fs));
          }
        }
      }
    }
  }

  private Calendar getDateFromTrashPath(String trashDatePath,
      String trashHourPath) {
    // Eg: TrashPath :: 2012-1-9
    String[] date = trashDatePath.split("-");
    String year = date[0];
    String month = date[1];
    String day = date[2];
    return CalendarHelper.getDateHour(year, month, day, trashHourPath);

  }

  private Map<String, Path> getStreamsInCluster(String root) throws Exception {
    Map<String, Path> streams = new HashMap<String, Path>();
    LOG.debug("Find streams in [" + root + "]");
    FileStatus[] paths = getAllFilesInDir(new Path(root), fs);
    if (paths != null) {
      for (FileStatus fileStatus : paths) {
        streams.put(fileStatus.getPath().getName(), fileStatus.getPath()
            .makeQualified(fs));
        LOG.debug("Purger working for stream [" + fileStatus.getPath() + "]");
      }
    } else
      LOG.debug("No streams found in [" + root + "]");
    return streams;
  }

  private void getStreamsPathToPurge(Map<String, Path> streamPathMap)
      throws Exception {
    Set<Map.Entry<String, Path>> streamsToProcess = streamPathMap.entrySet();
    Iterator it = streamsToProcess.iterator();
    while (it.hasNext()) {
      Map.Entry<String, Path> entry = (Map.Entry<String, Path>) it.next();
      String streamName = entry.getKey();
      Path streamRootPath = entry.getValue();
      LOG.debug("Find Paths to purge for stream [" + streamName
          + "] streamRootPath [" + streamRootPath + "]");
      // For each Stream, all years
      FileStatus[] years = getAllFilesInDir(streamRootPath, fs);
      if (years != null) {
        for (FileStatus year : years) {
          // For each month
          FileStatus[] months = getAllFilesInDir(year.getPath(), fs);
          if (months != null && months.length >= 1) {
            for (FileStatus month : months) {
              // For each day
              FileStatus[] days = getAllFilesInDir(month.getPath(), fs);
              if (days != null && days.length >= 1) {
                for (FileStatus day : days) {
                  // For each day
                  FileStatus[] hours = getAllFilesInDir(day.getPath(), fs);
                  if (hours != null && hours.length >= 1) {
                    for (FileStatus hour : hours) {
                      LOG.debug("Working for hour [" + hour.getPath() + "]");
                      Calendar streamDate = CalendarHelper.getDateHour(year
                          .getPath().getName(), month.getPath().getName(), day
                          .getPath().getName(), hour.getPath().getName());
                      LOG.debug("Validate [" + streamDate.toString()
                          + "] against retentionHours ["
                          + getRetentionPeriod(streamName) + "]");
                      if (isPurge(streamDate, getRetentionPeriod(streamName))) {
                        LOG.debug("Adding stream to purge [" + hour.getPath()
                            + "]");
                        streamsToPurge.add(hour.getPath().makeQualified(fs));
                      }
                    }
                  } else {
                    // No hour found in day. Purge day
                    streamsToPurge.add(day.getPath().makeQualified(fs));
                  }
                } // each day
              } else {
                // No day found in month. Purge month
                streamsToPurge.add(month.getPath().makeQualified(fs));
              }
            }// each month
          } else {
            // no months found in year. Purge Year.
            streamsToPurge.add(year.getPath().makeQualified(fs));
          }
        }// each year
      }
    }// each stream
  }

  public boolean isPurge(Calendar streamDate, Integer retentionPeriodinHours) {
    // int streamDay = streamDate.get(Calendar.DAY_OF_MONTH);
    Calendar nowTime = CalendarHelper.getNowTime();
    String streamDateStr = dateFormat.format(new Date(streamDate
        .getTimeInMillis()));
    String nowTimeStr = dateFormat.format(new Date(nowTime.getTimeInMillis()));

    LOG.debug("streamDate [" + streamDateStr + "] currentDate : [" + nowTimeStr
        + "] against retention [" + retentionPeriodinHours + " Hours]");

    LOG.debug("Hours between streamDate and nowTime is ["
        + getHoursBetweenDates(streamDate, nowTime) + " Hours]");
    if (getHoursBetweenDates(streamDate, nowTime) < retentionPeriodinHours)
      return false;
    else
      return true;
  }

  private int getHoursBetweenDates(Calendar startDate, Calendar endDate) {
    long diff = endDate.getTimeInMillis() - startDate.getTimeInMillis();
    int hours = (int) Math.floor(diff / MILLISECONDS_PER_HOUR);
    return Math.abs(hours);
  }

  private void purge() {
    Iterator it = streamsToPurge.iterator();
    Path purgePath = null;
    while (it.hasNext()) {
      try {
        purgePath = (Path) it.next();
        fs.delete(purgePath, true);
        LOG.info("Purging [" + purgePath + "]");
        ConduitMetrics.updateSWGuage(getServiceType(), PURGEPATHS_COUNT, getName(), 1);
      } catch (Exception e) {
        LOG.warn("Cannot delete path " + purgePath, e);
        ConduitMetrics.updateSWGuage(getServiceType(), DELETE_FAILURES_COUNT, getName(), 1);
      }
    }
  }

  private FileStatus[] getAllFilesInDir(Path dir, FileSystem fs)
      throws Exception {
    FileStatus[] files = null;
    try {
      files = fs.listStatus(dir);
    } catch (FileNotFoundException e) {

    }
    return files;
  }


  @Override
  protected String getTier() {
    throw new UnsupportedOperationException(" requested method is not" +
        " implemented in purger service");
  }

  @Override
  protected String getTopicNameFromDestnPath(Path destnPath) {
    throw new UnsupportedOperationException(" requested method is not" +
        " implemented in purger service");
  }
  /**
   * Get the service name 
   */
  public String getServiceType(){
    return "DataPurgerService";
  }
}
