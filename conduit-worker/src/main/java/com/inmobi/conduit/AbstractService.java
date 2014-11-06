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
package com.inmobi.conduit;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.inmobi.conduit.utils.CalendarHelper;

import com.inmobi.conduit.utils.HCatPartitionComparator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hive.hcatalog.api.HCatAddPartitionDesc;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatPartition;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.thrift.TSerializer;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.inmobi.audit.thrift.AuditMessage;
import com.inmobi.conduit.metrics.ConduitMetrics;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.publisher.MessagePublisher;
import com.inmobi.messaging.util.AuditUtil;

public abstract class AbstractService implements Service, Runnable {

  private static final Log LOG = LogFactory.getLog(AbstractService.class);
  protected static final long DEFAULT_RUN_INTERVAL = 60000;

  private final String name;
  protected final ConduitConfig config;
  protected final long runIntervalInMsec;
  protected Thread thread;
  protected volatile boolean stopped = false;
  protected CheckpointProvider checkpointProvider = null;
  protected static final int DEFAULT_WINDOW_SIZE = 60;
  private final TSerializer serializer = new TSerializer();
  protected final static long MILLISECONDS_IN_MINUTE = 60 * 1000;
  protected static final int NANO_SECONDS_IN_SECOND = 1000 * 1000 * 1000;
  private Map<String, Long> prevRuntimeForCategory = new HashMap<String, Long>();
  protected final SimpleDateFormat LogDateFormat = new SimpleDateFormat(
      "yyyy/MM/dd, hh:mm");
  protected final Set<String> streamsToProcess;
  protected final Map<String, Long> lastProcessedFile;
  private final static long TIME_RETRY_IN_MILLIS = 500;
  protected int numOfRetries;
  protected Path tmpCounterOutputPath;
  public final static String RUNTIME = "runtime";
  public final static String FAILURES = "failures";
  public final static String COMMIT_TIME = "commit.time";
  public final static String RETRY_RENAME = "retry.rename";
  public final static String RETRY_MKDIR = "retry.mkDir";
  public final static String EMPTYDIR_CREATE = "emptyDir.create";
  public final static String RETRY_CHECKPOINT = "retry.checkPoint";
  public final static String FILES_COPIED_COUNT = "filesCopied.count";
  public final static String DATAPURGER_SERVICE = "DataPurgerService";
  public final static String LAST_FILE_PROCESSED = "lastfile.processed";
  public final static String JOB_EXECUTION_TIME = "job.execution.time";
  public final static String HCAT_ADD_PARTITIONS_COUNT = "hcat.addpartitions.count";
  public final static String HCAT_CONNECTION_FAILURES = "hcat.connection.failures";
  public final static String FAILED_TO_GET_HCAT_CLIENT_COUNT = "failed.hcatclient";
  protected static final String TABLE_PREFIX = "conduit";
  protected static final String TABLE_NAME_SEPARATOR = "_";
  protected static final String LOCAL_TABLE_PREFIX = TABLE_PREFIX
      + TABLE_NAME_SEPARATOR +"local";
  protected static final long EMPTY_PARTITION_LIST = -1;
  protected static final long FAILED_GET_PARTITIONS = -2;
  protected final static Map<String, Boolean> streamHcatEnableMap =
      new ConcurrentHashMap<String, Boolean>();
  protected final static Map<String, Long> lastAddedPartitionMap =
      new ConcurrentHashMap<String, Long>();
  protected final static Map<String, List<Path>> pathsToBeregisteredPerTable =
      new ConcurrentHashMap<String, List<Path>>();

  protected  HCatClientUtil hcatUtil;

  protected static String hostname;
  static {
    try {
      hostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOG.error("Unable to find the hostanme of the worker box,audit packets"
          + " won't contain hostname");
      hostname = "";
    }
  }

  public AbstractService(String name, ConduitConfig config,
      Set<String> streamsToProcess, HCatClientUtil hcatUtil) {
    this(name, config, DEFAULT_RUN_INTERVAL,streamsToProcess, hcatUtil);
  }

  public AbstractService(String name, ConduitConfig config,
      long runIntervalInMsec, Set<String> streamsToProcess,
      HCatClientUtil hcatUtil) {
    this.config = config;
    this.name = name;
    this.runIntervalInMsec = runIntervalInMsec;
    String retries = System.getProperty(ConduitConstants.NUM_RETRIES);
    this.streamsToProcess=streamsToProcess;
    this.lastProcessedFile = new HashMap<String, Long>();
    if (retries == null) {
      numOfRetries = Integer.MAX_VALUE;
    } else {
      numOfRetries = Integer.parseInt(retries);
    }
    this.hcatUtil = hcatUtil;
  }

  public AbstractService(String name, ConduitConfig config,
      long runIntervalInMsec, CheckpointProvider provider,
      Set<String> streamsToProcess, HCatClientUtil hcatUtil) {
    this(name, config, runIntervalInMsec, streamsToProcess, hcatUtil);
    this.checkpointProvider = provider;
  }

  protected final static String getServiceName(Set<String> streamsToProcess) {
    StringBuffer serviceName = new StringBuffer("");
    for (String stream : streamsToProcess) {
      serviceName.append(stream).append("@");
    }
    return serviceName.toString();
  }

  public ConduitConfig getConfig() {
    return config;
  }

  public String getName() {
    return name;
  }

  public abstract long getMSecondsTillNextRun(long currentTime);

  protected abstract void execute() throws Exception;

  public static String getCheckPointKey(String serviceName, String stream,
      String source) {
    return serviceName + "_" + stream + "_" + source;
  }

  protected void preExecute() throws Exception {
  }

  protected void postExecute() throws Exception {
  }


  @Override
  public void run() {
    LOG.info("Starting Service [" + Thread.currentThread().getName() + "]");
    while (!stopped) {
      long startTime = System.currentTimeMillis();
      try {
        LOG.info("Performing Pre Execute Step before a run...");
        preExecute();
        LOG.info("Starting a run...");
        execute();
        LOG.info("Performing Post Execute Step after a run...");
        postExecute();
        if (stopped)
          return;
      } catch (Throwable th) {
        if (!DATAPURGER_SERVICE.equalsIgnoreCase(getServiceType())) {
          for (String eachStream : streamsToProcess) {
            ConduitMetrics.updateSWGuage(getServiceType(), FAILURES,
                eachStream, 1);
          }
        } else {
          ConduitMetrics.updateSWGuage(getServiceType(), FAILURES,
              Thread.currentThread().getName(), 1);
        }
        LOG.error("Thread: " + thread + " interrupt status: "
            + thread.isInterrupted() + " and Error in run: ", th);
      }
      long finishTime = System.currentTimeMillis();
      long elapsedTime = finishTime - startTime;
      if (!DATAPURGER_SERVICE.equalsIgnoreCase(getServiceType())) {
        for (String eachStream : streamsToProcess) {
          ConduitMetrics.updateSWGuage(getServiceType(), RUNTIME, eachStream,
              elapsedTime);
        }
      } else {
        ConduitMetrics.updateSWGuage(getServiceType(), RUNTIME,
            Thread.currentThread().getName(), elapsedTime);
      }
      if (elapsedTime >= runIntervalInMsec)
        continue;
      else {
        try {
          long sleepTime = getMSecondsTillNextRun(finishTime);
          if (sleepTime > 0) {
            LOG.info("Sleeping for " + sleepTime);
            Thread.sleep(sleepTime);
          }
        } catch (InterruptedException e) {
          LOG.warn("thread interrupted " + thread.getName(), e);
        }
      }
    }
  }

  @Override
  public synchronized void start() {
    thread = new Thread(this, this.name);
    LOG.info("Starting thread " + thread.getName());
    thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {

      public void uncaughtException(Thread t, Throwable e) {
        LOG.error("Thread: " + thread + " Uncaught handler:" +
            " Thread interrupt status: " + thread.isInterrupted()
            + " and exception caught is: " + e);
      }
    });
    thread.start();
  }

  @Override
  public void stop() {
    stopped = true;
    LOG.info(Thread.currentThread().getName() + " stopped [" + stopped + "]");
  }

  @Override
  public synchronized void join() {
    try {
      if (thread != null) {
        thread.join();
      } else {
        LOG.warn("service " + this.getName() +  " not started hence returning" +
            " from join()");
      }
    } catch (InterruptedException e) {
      LOG.warn("thread interrupted " + thread.getName());
    }
  }

  protected void updateJobTimeCounter(long jobExecutionTime) {
    for (String eachStream : streamsToProcess) {
      ConduitMetrics.updateSWGuage(getServiceType(), JOB_EXECUTION_TIME,
          eachStream, jobExecutionTime);
    }
  }

  protected String getLogDateString(long commitTime) {
    return LogDateFormat.format(commitTime);
  }

  protected HCatClient getHCatClient() throws InterruptedException {
    return hcatUtil.getHCatClient();
  }

  protected void addToPool(HCatClient hcatClient) {
    if (hcatClient != null) {
      hcatUtil.addToPool(hcatClient);
    }
  }

  private Path getLatestDir(FileSystem fs, Path Dir) throws Exception {

    FileStatus[] fileStatus;
    try {
      fileStatus = fs.listStatus(Dir);
    } catch (FileNotFoundException fe) {
      fileStatus = null;
    }
    if (fileStatus != null && fileStatus.length > 0) {
      FileStatus latestfile = fileStatus[0];
      for (FileStatus currentfile : fileStatus) {
        if (currentfile.getPath().getName()
            .compareTo(latestfile.getPath().getName()) > 0)
          latestfile = currentfile;
      }
      return latestfile.getPath();
    }
    return null;
  }

  private long getPreviousRuntime(FileSystem fs, String destDir, String category)
      throws Exception {
    String localDestDir = destDir + File.separator + category;
    LOG.warn("Querying Directory [" + localDestDir + "]");
    Path latestyeardir = getLatestDir(fs, new Path(localDestDir));
    int latestyear = 0, latestmonth = 0, latestday = 0, latesthour = 0, latestminute = 0;

    if (latestyeardir != null) {
      latestyear = Integer.parseInt(latestyeardir.getName());
      Path latestmonthdir = getLatestDir(fs, latestyeardir);
      if (latestmonthdir != null) {
        latestmonth = Integer.parseInt(latestmonthdir.getName());
        Path latestdaydir = getLatestDir(fs, latestmonthdir);
        if (latestdaydir != null) {
          latestday = Integer.parseInt(latestdaydir.getName());
          Path latesthourdir = getLatestDir(fs, latestdaydir);
          if (latesthourdir != null) {
            latesthour = Integer.parseInt(latesthourdir.getName());
            Path latestminutedir = getLatestDir(fs, latesthourdir);
            if (latestminutedir != null) {
              latestminute = Integer.parseInt(latestminutedir.getName());
            }
          }
        }
      }
    } else
      return -1;
    LOG.debug("Date Found " + latestyear + File.separator + latestmonth
        + File.separator + latestday + File.separator + latesthour
        + File.separator + latestminute);
    return CalendarHelper.getDateHourMinute(latestyear, latestmonth, latestday,
        latesthour, latestminute).getTimeInMillis();
  }

  protected void publishMissingPaths(FileSystem fs, String destDir,
      long commitTime, String categoryName) throws Exception {
    Long prevRuntime = new Long(-1);
    if (!prevRuntimeForCategory.containsKey(categoryName)) {
      LOG.debug("Calculating Previous Runtime from Directory Listing");
      prevRuntime = getPreviousRuntime(fs, destDir, categoryName);
    } else {
      LOG.debug("Reading Previous Runtime from Cache");
      prevRuntime = prevRuntimeForCategory.get(categoryName);
    }

    if (prevRuntime != -1) {
      if (isMissingPaths(commitTime, prevRuntime)) {
        LOG.debug("Previous Runtime: [" + getLogDateString(prevRuntime) + "]");
        List<Path> pathsToBeRegistered = null;
        String tableName = null;
        if (isStreamHCatEnabled(categoryName)) {
          tableName = getTableName(categoryName);
          pathsToBeRegistered = pathsToBeregisteredPerTable.get(tableName);
        }
        while (isMissingPaths(commitTime, prevRuntime)) {
          String missingPath = Cluster.getDestDir(destDir, categoryName,
              prevRuntime);
          Path missingDir = new Path(missingPath);
          if (!fs.exists(missingDir)) {
            LOG.debug("Creating Missing Directory [" + missingDir + "]");
            fs.mkdirs(missingDir);
            if (isStreamHCatEnabled(categoryName)) {
              synchronized (pathsToBeRegistered) {
                pathsToBeRegistered.add(missingDir);
              }
            }
            ConduitMetrics.updateSWGuage(getServiceType(), EMPTYDIR_CREATE,
                categoryName, 1);
          }
          prevRuntime += MILLISECONDS_IN_MINUTE;
        }
        if (isStreamHCatEnabled(categoryName)) {
          pathsToBeregisteredPerTable.put(tableName, pathsToBeRegistered);
        }
      }
    }
    // prevRuntimeForCategory map is updated with commitTime,
    // even if prevRuntime is -1, since service did run at this point
    prevRuntimeForCategory.put(categoryName, commitTime);
  }

  private boolean preparePartitionsTobeRegistered(String streamName)
      throws InterruptedException {

    String tableName = getTableName(streamName);
    long lastAddedTime = lastAddedPartitionMap.get(tableName);
    LOG.info("Last added partition time " + getLogDateString(lastAddedTime)
        + " for table " + tableName);
    if (lastAddedTime == EMPTY_PARTITION_LIST) {
      LOG.info("there are no partitions in "+ tableName +" table. ");
      return true;
    } else if (lastAddedTime == FAILED_GET_PARTITIONS) {
      HCatClient hcatClient = getHCatClient();
      if (hcatClient == null) {
        LOG.warn("Didn't get any hcat client from pool hence not preparing"
            + " final partition list");
        ConduitMetrics.updateSWGuage(getServiceType(), FAILED_TO_GET_HCAT_CLIENT_COUNT,
            streamName, 1);
        return false;
      }
      try {
        findLastPartition(hcatClient, streamName);
        lastAddedTime = lastAddedPartitionMap.get(tableName);
        LOG.info("Last added parittion time " + getLogDateString(lastAddedTime)
            + " for table " + tableName);
        if (lastAddedTime == EMPTY_PARTITION_LIST) {
          LOG.info("there are no partitions in "+ tableName +" table. ");
          return true;
        }
      } catch (HCatException e) {
        LOG.error("Got exception while trying to get the last added partition ", e);
        return false;
      } finally {
        addToPool(hcatClient);
      }
    }
    findDiffBetweenLastAddedAndFirstPath(lastAddedTime, streamName, tableName);
    return true;
  }

  private void findDiffBetweenLastAddedAndFirstPath(long lastAddedTime,
      String stream, String table) {
    List<Path> listOfPathsTobeRegistered = pathsToBeregisteredPerTable.get(table);
    synchronized (listOfPathsTobeRegistered) {
      if (listOfPathsTobeRegistered.isEmpty()) {
        return;
      } else {
        // get the first path
        Path firstPathInList = listOfPathsTobeRegistered.get(0);
        Date timeFromPath = getTimeStampFromHCatPartition(firstPathInList.toString(),
            stream);
        LOG.info("Find the missing partitions between "
            + getLogDateString(lastAddedTime) + " and " + timeFromPath
            + " for table " + table);
        while (isMissingPartitions(timeFromPath.getTime(), lastAddedTime)) {
          long nextPathPartTime = lastAddedTime + MILLISECONDS_IN_MINUTE;
          Path nextPathTobeAdded = getFinalPath(nextPathPartTime, stream);
          LOG.info("Add the missing partition location " + nextPathTobeAdded
              + " to the list for registering");
          if (nextPathTobeAdded != null) {
            listOfPathsTobeRegistered.add(nextPathTobeAdded);
            lastAddedTime = nextPathPartTime;
          }
        }
        Collections.sort(listOfPathsTobeRegistered);
        pathsToBeregisteredPerTable.put(table, listOfPathsTobeRegistered);
      }
    }
  }

  protected Path getFinalPath(long time, String stream) {
    return null;
  }

  public void prepareLastAddedPartitionMap() throws InterruptedException {
    prepareStreamHcatEnableMap();

    HCatClient hcatClient = getHCatClient();
    if (hcatClient == null) {
      LOG.warn("Did not get hcatclient hence not finding the last added partition");
      for (String stream : streamsToProcess) {
        if (isStreamHCatEnabled(stream)) {
          updateLastAddedPartitionMap(getTableName(stream), FAILED_GET_PARTITIONS);
          ConduitMetrics.updateSWGuage(getServiceType(),
              FAILED_TO_GET_HCAT_CLIENT_COUNT, stream, 1);
        }
      }
      return;
    }
    try {
      for (String stream : streamsToProcess) {
        if (isStreamHCatEnabled(stream)) {
          try {
            hcatClient.getTable(Conduit.getHcatDBName(), getTableName(stream));
            findLastPartition(hcatClient, stream);
          } catch (HCatException e) {
            if (e.getCause() instanceof NoSuchObjectException) {
              LOG.error("Got noSuchObject exception while trying to get table"
                  + " or finding last partition " + e.getMessage());
              throw new RuntimeException(e);
            }
            LOG.warn("Got Exception while finding the last added partition for"
                + " stream " + stream, e);
            updateLastAddedPartitionMap(getTableName(stream), FAILED_GET_PARTITIONS);
          }
        } else {
          LOG.debug("Hcatalog is not enabled for " + stream + " stream");
        }
      }
    } finally {
      addToPool(hcatClient);
    }
  }

  /*
   * It finds the last added partition from the hcatalog table for each stream.
   * Update with -1 if there are no partitions present in hcatalog.
   */
  public void findLastPartition(HCatClient hcatClient, String stream)
      throws HCatException {
    String tableName = getTableName(stream);
    List<HCatPartition> hCatPartitionList = hcatClient.getPartitions(
        Conduit.getHcatDBName(), tableName);
    if (hCatPartitionList.isEmpty()) {
      LOG.info("No partitions present for " + stream + " stream. ");
      updateLastAddedPartitionMap(tableName, EMPTY_PARTITION_LIST);
      return;
    }
    Collections.sort(hCatPartitionList, new HCatPartitionComparator());
    HCatPartition lastHcatPartition = hCatPartitionList.get(hCatPartitionList.size()-1);
    Date lastAddedPartitionDate = getTimeStampFromHCatPartition(
        lastHcatPartition.getLocation(), stream);
    if (lastAddedPartitionDate != null) {
      LOG.info("Last added partition timetamp : " + lastAddedPartitionDate
          + " for table " + tableName);
      updateLastAddedPartitionMap(tableName, lastAddedPartitionDate.getTime());
    } else {
      updateLastAddedPartitionMap(tableName, EMPTY_PARTITION_LIST);
    }
  }

  protected abstract String getTableName(String stream);

  protected Date getTimeStampFromHCatPartition(String hcatLoc, String stream) {
    return null;
  }

  protected boolean isStreamHCatEnabled(String stream) {
    return streamHcatEnableMap.containsKey(stream)
        && streamHcatEnableMap.get(stream);
  }

  protected void updateLastAddedPartitionMap(String tableName, long partTime) {
    lastAddedPartitionMap.put(tableName, partTime);
  }

  protected void updateStreamHCatEnabledMap(String stream, boolean hcatEnabled) {
    streamHcatEnableMap.put(stream, hcatEnabled);
  }

  protected void prepareStreamHcatEnableMap() {
    // override in local, merge and mirror stream services
  }

  protected void registerPartitions() throws InterruptedException, ParseException {
    if (!Conduit.isHCatEnabled()) {
      return;
    }
    HCatClient hcatClient = getHCatClient();
    if (hcatClient == null) {
      LOG.warn("Did not get hcat client hence not rgistering partitions");
      for (String stream : streamsToProcess) {
        ConduitMetrics.updateSWGuage(getServiceType(),
            FAILED_TO_GET_HCAT_CLIENT_COUNT, stream, 1);
      }
      return;
    }
    try {
      for (String stream : streamsToProcess) {
        if (!isStreamHCatEnabled(stream)) {
          LOG.info("Hcat is not enabled for " + stream + " stream."
              + " Hence not registering partitions");
          continue;
        }
        String tableName = getTableName(stream);
        /*
         * If it is not able to find the diff between the last added partition
         * and first path in the partition list then it should not register
         * partitions until it finds the diff
         */
        if (!preparePartitionsTobeRegistered(stream)) {
          LOG.info("Not registering the partitions as part of this run as"
              + " it was not able to find the last added partition"
              + " or diff betweeen last added and first path in the list" );
          continue;
        }
        if (lastAddedPartitionMap.get(tableName) == FAILED_GET_PARTITIONS) {
          LOG.warn("Failed to get partitions for stream from server hence"
              + " not registering new partiotions");
          continue;
        }
        List<Path> partitionsTobeRegistered = pathsToBeregisteredPerTable.get(tableName);
        synchronized (partitionsTobeRegistered) {
          // Register all the partitions in the list except the last one
          while (partitionsTobeRegistered.size() > 1) {
            /* always retrieve first element from the list as we remove the
             * element once it is added to partition. then second element will
             *  be the first one
             */
            Path path = partitionsTobeRegistered.get(0);
            Date date = getTimeStampFromHCatPartition(path.toString(), stream);
            LOG.info("Adding the partition  " + path.toString() + " in " + tableName + " table");
            if (addPartition(path.toString(), stream, date.getTime(), tableName,
                hcatClient)) {
              partitionsTobeRegistered.remove(0);
              updateLastAddedPartitionMap(tableName, date.getTime());
            } else {
              break;
            }
          }
        }
      }
    } finally {
      addToPool(hcatClient);
    }
  }

  public boolean addPartition(String location, String streamName,
      long partTimeStamp, String tableName, HCatClient hcatClient)
          throws InterruptedException, ParseException {
    if (hcatClient == null) {
      LOG.warn("Did not get hcat client for table " + tableName);
      return false;
    }
    String dbName = Conduit.getHcatDBName();
    String dateStr = Cluster.getDateAsYYYYMMDDHHMNPath(partTimeStamp);
    String [] dateSplits = dateStr.split(File.separator);
    Map<String, String> partSpec = new HashMap<String, String>();
    if (dateSplits.length == 5) {
      partSpec.put("year", dateSplits[0]);
      partSpec.put("month", dateSplits[1]);
      partSpec.put("day", dateSplits[2]);
      partSpec.put("hour", dateSplits[3]);
      partSpec.put("minute", dateSplits[4]);
    }
    HCatAddPartitionDesc partInfo = null;
    try {
      if (partInfo == null) {
        partInfo = HCatAddPartitionDesc.create(dbName, tableName, location,
            partSpec).build();
      }
      hcatClient.addPartition(partInfo);
      LOG.info("Partition " + partInfo.getLocation() + " was added successfully");
      ConduitMetrics.updateSWGuage(getServiceType(), HCAT_ADD_PARTITIONS_COUNT,
          streamName, 1);
      return true;
    } catch (HCatException e) {
      if (e.getCause() instanceof AlreadyExistsException) {
        LOG.warn("Partition " + partInfo.getLocation() + " is already exists in "
            + tableName + " table. ", e);
        return true;
      }
      ConduitMetrics.updateSWGuage(getServiceType(), HCAT_CONNECTION_FAILURES,
          getName(), 1);
      LOG.info("Got Exception while trying to add partition  : " + partInfo
          + ". Exception ", e);
    }
    return false;
  }

  /*
   * Retries renaming a file to a given num of times defined by
   * "com.inmobi.conduit.retries" system property Returns the outcome of last
   * retry;throws exception in case last retry threw an exception
   */
  protected boolean retriableRename(FileSystem fs, Path src, Path dst,
      String streamName) throws Exception {
    int count = 0;
    boolean result = false;
    Exception exception = null;
    while (count < numOfRetries) {
      try {
        result = fs.rename(src, dst);
        exception = null;
        break;
      } catch (Exception e) {
        LOG.warn("Moving " + src + " to " + dst + " failed.Retrying ", e);
        exception = e;
        if (stopped)
          break;
      }
      count++;
      if (streamName != null) {
        ConduitMetrics.updateSWGuage(getServiceType(), RETRY_RENAME, streamName, 1);
      } else {
        LOG.warn("Can not increment retriable rename gauge as stream name is null");
      }
      try {
        Thread.sleep(TIME_RETRY_IN_MILLIS);
      } catch (InterruptedException e) {
        LOG.warn(e);
      }
    }
    if (count == numOfRetries) {
      LOG.error("Max retries done for moving " + src + " to " + dst
          + " quitting now");
    }
    if (exception == null) {
      return result;
    } else {
      throw exception;
    }
  }

  protected boolean retriableDelete(FileSystem fs, Path path) throws Exception {
    int count = 0;
    boolean result = false;
    Exception exception = null;
    while (count < numOfRetries) {
      try {
        result = fs.delete(path, false);
        exception = null;
        break;

      } catch (Exception e) {
        LOG.warn("Couldn't delete path " + path + " .Retrying ", e);
        exception = e;
        if (stopped)
          break;
      }
      count++;
      try {
        Thread.sleep(TIME_RETRY_IN_MILLIS);
      } catch (InterruptedException e) {
        LOG.warn(e);
      }
    }
    if (count == numOfRetries) {
      LOG.error("Max retries done for deleting " + path + " quitting");
    }
    if (exception == null) {
      return result;
    } else {
      throw exception;
    }

  }

  protected void retriableCheckPoint(CheckpointProvider provider, String key,
      byte[] checkpoint, String streamName) throws Exception {
    int count = 0;
    Exception ex = null;
    while (count < numOfRetries) {
      try {
        provider.checkpoint(key, checkpoint);
        ex = null;
        break;
      } catch (Exception e) {
        LOG.warn("Couldn't checkpoint key " + key + " .Retrying ", e);
        ex = e;
        if (stopped)
          break;
      }
      count++;
      if (streamName != null) {
        ConduitMetrics.updateSWGuage(getServiceType(), RETRY_CHECKPOINT,
            streamName, 1);
      } else {
        LOG.warn("Can not increment retriable checkpoint gauge as stream name is null");
      }
      try {
        Thread.sleep(TIME_RETRY_IN_MILLIS);
      } catch (InterruptedException e) {
        LOG.error(e);
      }
    }
    if (count == numOfRetries) {
      LOG.error("Max retries done for checkpointing for key " + key);
    }
    if (ex != null)
      throw ex;
  }

  protected boolean retriableMkDirs(FileSystem fs, Path p, String streamName)
      throws Exception {
    int count = 0;
    boolean result = false;
    Exception ex = null;
    while (count < numOfRetries) {
      try {
        result = fs.mkdirs(p);
        ex = null;
        break;

      } catch (Exception e) {
        LOG.warn("Couldn't make directories for path " + p + " .Retrying ", e);
        ex = e;
        if (stopped)
          break;
      }
      count++;
      if (streamName != null) {
        ConduitMetrics.updateSWGuage(getServiceType(), RETRY_MKDIR, streamName, 1);
      } else {
        LOG.warn("Can not increment retriable mkdir gauge as stream name is null");
      }
      try {
        Thread.sleep(TIME_RETRY_IN_MILLIS);
      } catch (InterruptedException e) {
        LOG.warn(e);
      }
    }
    if (count == numOfRetries) {
      LOG.error("Max retries done for mkdirs " + p + " quitting");
    }
    if (ex == null)
      return result;
    else
      throw ex;
  }

  protected boolean retriableExists(FileSystem fs, Path p, String streamName)
      throws Exception {
    int count = 0;
    boolean result = false;
    Exception ex = null;
    while (count < numOfRetries) {
      try {
        result = fs.exists(p);
        ex = null;
        break;
      } catch (Exception e) {
        LOG.warn("Error while checking for existence of " + p + " .Retrying ",
            e);
        ex = e;
        if (stopped)
          break;
      }
      count++;
      try {
        Thread.sleep(TIME_RETRY_IN_MILLIS);
      } catch (InterruptedException e) {
        LOG.error(e);
      }
    }
    if (count == numOfRetries) {
      LOG.error("Max retries done for mkdirs " + p + " quitting");
    }
    if (ex == null)
      return result;
    else
      throw ex;
  }

  private boolean isMissingPaths(long commitTime, long prevRuntime) {
    return ((commitTime - prevRuntime) >= MILLISECONDS_IN_MINUTE);
  }

  protected boolean isMissingPartitions(long commitTime, long lastAddedPartTime) {
    return ((commitTime - lastAddedPartTime) > MILLISECONDS_IN_MINUTE);
  }

  protected void publishMissingPaths(FileSystem fs, String destDir,
      long commitTime, Set<String> streams) throws Exception {
    if (streams != null) {
      for (String category : streams) {
        publishMissingPaths(fs, destDir, commitTime, category);
      }
    }
  }

  /**
   * Get the service name from the name
   */
  abstract public String getServiceType();

  private List<Path> listPartFiles(Path path, FileSystem fs) {
    List<Path> matches = new LinkedList<Path>();
    try {
      FileStatus[] statuses = fs.listStatus(path, new PathFilter() {
        public boolean accept(Path path) {
          return path.toString().contains("part");
        }
      });
      for (FileStatus status : statuses) {
        matches.add(status.getPath());
      }
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
    }
    return matches;
  }

  protected Table<String, Long, Long> parseCountersFile(FileSystem fs) {
    List<Path> partFiles = listPartFiles(tmpCounterOutputPath, fs);
    if (partFiles == null || partFiles.size() == 0) {
      LOG.warn("No counters files generated by mapred job");
      return null;
    }
    Table<String, Long, Long> result = HashBasedTable.create();
    for (Path filePath : partFiles) {
      FSDataInputStream fin = null;
      Scanner scanner = null;
      try {
        fin = fs.open(filePath);
        scanner = new Scanner(fin);

        while (scanner.hasNext()) {
          String counterNameValue = null;
          try {
            counterNameValue = scanner.next();
            String tmp[] = counterNameValue.split(ConduitConstants.
                AUDIT_COUNTER_NAME_DELIMITER);
            if (tmp.length < 4) {
              LOG.error("Malformed counter name,skipping " + counterNameValue);
              continue;
            }
            String streamFileNameCombo = tmp[0]
                + ConduitConstants.AUDIT_COUNTER_NAME_DELIMITER + tmp[1];
            Long publishTimeWindow = Long.parseLong(tmp[2]);
            Long numOfMsgs = Long.parseLong(tmp[3]);
            result.put(streamFileNameCombo, publishTimeWindow, numOfMsgs);
          } catch (Exception e) {
            LOG.error("Counters file has malformed line with counter name = "
                + counterNameValue + " ..skipping the line", e);
          }
        }
      } catch (IOException e1) {
        LOG.error("Error while opening file " + filePath + " Skipping");
        continue;
      } finally {
        try {
          if (fin != null) {
            fin.close();
          }
          if (scanner != null) {
            scanner.close();
          }
        } catch (Exception e) {
          LOG.warn("Error while closing file " + filePath + " or scanner");
        }
      }
    }
    return result;

  }

  protected AuditMessage createAuditMessage(String streamName,
      Map<Long, Long> received) {

    AuditMessage auditMsg = new AuditMessage(new Date().getTime(), streamName,
        getTier(),
        hostname, DEFAULT_WINDOW_SIZE, received, null, null, null);
    return auditMsg;
  }

  abstract protected String getTopicNameFromDestnPath(Path destnPath);

  abstract protected String getTier();

  protected void generateAuditMsgs(String streamName, String fileName,
      Table<String, Long, Long> parsedCounters, List<AuditMessage> auditMsgList) {
    if (Conduit.getPublisher() == null) {
      LOG.debug("Not generating audit messages as publisher is null");
      return;
    }
    if (parsedCounters == null) {
      LOG.error("Not generating audit message for stream " +  streamName +
          " as parsed counters are null");
      return;
    }
    if (streamName.equals(AuditUtil.AUDIT_STREAM_TOPIC_NAME)) {
      LOG.debug("Not generating audit for audit stream");
      return;
    }
    String streamFileNameCombo = streamName + ConduitConstants.
        AUDIT_COUNTER_NAME_DELIMITER + fileName;
    Map<Long, Long> received = parsedCounters.row(streamFileNameCombo);
    if (!received.isEmpty()) {
      // create audit message
      AuditMessage auditMsg = createAuditMessage(streamName, received);
      auditMsgList.add(auditMsg);
    } else {
      LOG.info("Not publishing audit packet for stream " + streamName +
          " as counters are empty");
    }
  }

  protected void publishAuditMessages(List<AuditMessage> auditMsgList){
    for (AuditMessage auditMsg : auditMsgList) {
      try {
        LOG.debug("Publishing audit message " + auditMsg.toString() + " from "
            + this.getClass().getSimpleName());
        MessagePublisher publisher = Conduit.getPublisher();
        publisher.publish(AuditUtil.AUDIT_STREAM_TOPIC_NAME,
            new Message(ByteBuffer.wrap(serializer.serialize(auditMsg))));
      } catch (Exception e) {
        LOG.error("Publishing of audit message " + auditMsg.toString() +
            " failed ", e);
      }
    }
  }

  /*
   * This method is only for test cases
   */
  public static void clearHCatInMemoryMaps() {
    streamHcatEnableMap.clear();
    lastAddedPartitionMap.clear();
  }

  // this is for only tests
  public void clearPathPartitionTable() {
    for (String stream : streamsToProcess) {
      String tableName = getTableName(stream);
      pathsToBeregisteredPerTable.get(tableName).clear();
    }
  }
}

