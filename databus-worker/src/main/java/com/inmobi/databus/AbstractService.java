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
package com.inmobi.databus;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.inmobi.audit.thrift.AuditMessage;
import com.inmobi.databus.local.CopyMapper;
import com.inmobi.databus.utils.CalendarHelper;
import com.inmobi.messaging.publisher.MessagePublisher;

public abstract class AbstractService implements Service, Runnable {

  private static final Log LOG = LogFactory.getLog(AbstractService.class);
  protected static final long DEFAULT_RUN_INTERVAL = 60000;

  private final String name;
  private final DatabusConfig config;
  protected final long runIntervalInMsec;
  protected Thread thread;
  protected volatile boolean stopped = false;
  protected CheckpointProvider checkpointProvider = null;
	private final static long MILLISECONDS_IN_MINUTE = 60 * 1000;
	private Map<String, Long> prevRuntimeForCategory = new HashMap<String, Long>();
	protected final SimpleDateFormat LogDateFormat = new SimpleDateFormat(
	    "yyyy/MM/dd, hh:mm");
	private final static long MILLISECONDS_IN_HOUR = 60 * MILLISECONDS_IN_MINUTE;
  protected String hostname;
  protected static final int DEFAULT_WINDOW_SIZE = 60;
  protected final MessagePublisher publisher;
  protected final static char TOPIC_SEPARATOR_FILENAME = '-';


  public AbstractService(String name, DatabusConfig config,
      MessagePublisher publisher) {
    this(name, config, DEFAULT_RUN_INTERVAL, publisher);
  }

  public AbstractService(String name, DatabusConfig config,
      long runIntervalInMsec, MessagePublisher publisher) {
    this.config = config;
    this.name = name;
    this.runIntervalInMsec = runIntervalInMsec;
    try {
      hostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOG.error("Unable to find the hostanme of the worker box,audit packets"
          + " won't contain hostname");
      hostname = "";
    }
    this.publisher = publisher;
  }

  public AbstractService(String name, DatabusConfig config,
      long runIntervalInMsec, CheckpointProvider provider,
      MessagePublisher publisher) {
    this(name, config, runIntervalInMsec, publisher);
    this.checkpointProvider = provider;
  }

  public DatabusConfig getConfig() {
    return config;
  }

  public String getName() {
    return name;
  }

  public abstract long getMSecondsTillNextRun(long currentTime);

  protected abstract void execute() throws Exception;
  
  protected void preExecute() throws Exception {
  }
  
  protected void postExecute() throws Exception {
  }

  @Override
  public void run() {
    LOG.info("Starting Service [" + Thread.currentThread().getName() + "]");
    while (!stopped && !thread.isInterrupted()) {
      long startTime = System.currentTimeMillis();
      try {
        LOG.info("Performing Pre Execute Step before a run...");
        preExecute();
        LOG.info("Starting a run...");
        execute();
        LOG.info("Performing Post Execute Step after a run...");
        postExecute();
        if (stopped || thread.isInterrupted())
          return;
      } catch (Exception e) {
        LOG.warn("Error in run", e);
      }
      long finishTime = System.currentTimeMillis();
      long elapsedTime = finishTime - startTime;
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
          return;
        }
      }
    }
  }

  @Override
  public synchronized void start() {
    thread = new Thread(this, this.name);
    LOG.info("Starting thread " + thread.getName());
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
      thread.join();
    } catch (InterruptedException e) {
      LOG.warn("thread interrupted " + thread.getName());
    }
  }

	protected String getLogDateString(long commitTime) {
		return LogDateFormat.format(commitTime);
	}

	private Path getLatestDir(FileSystem fs, Path Dir) throws Exception {
		FileStatus[] fileStatus = fs.listStatus(Dir);

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

	private boolean isMissingPaths(long commitTime, long prevRuntime) {
		return ((commitTime - prevRuntime) >= MILLISECONDS_IN_MINUTE);
	}

  protected Set<Path> publishMissingPaths(FileSystem fs, String destDir,
	    long commitTime, String categoryName) throws Exception {
    Set<Path> missingDirectories = new TreeSet<Path>();
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
				while (isMissingPaths(commitTime, prevRuntime)) {
					String missingPath = Cluster.getDestDir(destDir, categoryName,
					    prevRuntime);
          Path missingDir = new Path(missingPath);
          if (!fs.exists(missingDir)) {
            missingDirectories.add(new Path(missingPath));
          }
					prevRuntime += MILLISECONDS_IN_MINUTE;
				}
			}
		}
    return missingDirectories;
	}

  protected Map<String, Set<Path>> publishMissingPaths(FileSystem fs,
      String destDir, long commitTime)
	    throws Exception {
    Map<String, Set<Path>> missingDirectories = new HashMap<String, Set<Path>>();
    Set<Path> missingdirsinstream = null;
		FileStatus[] fileStatus = fs.listStatus(new Path(destDir));
		LOG.info("Create All the Missing Paths in " + destDir);
		if (fileStatus != null) {
			for (FileStatus file : fileStatus) {
        missingdirsinstream = publishMissingPaths(fs, destDir,
            commitTime, file.getPath().getName());
        if (missingdirsinstream.size() > 0)
          missingDirectories.put(file.getPath().getName(), missingdirsinstream);
			}
		}
		LOG.info("Done Creating All the Missing Paths in " + destDir);
    return missingDirectories;
	}
  
  /*
   * publish all the missing paths and clears missingDirCommittedPaths map
   * after publishing
   */
  public void commitPublishMissingPaths(FileSystem fs, 
      Map<String, Set<Path>> missingDirsCommittedPaths, long commitTime) 
          throws IOException {
    if (missingDirsCommittedPaths != null && missingDirsCommittedPaths.size() > 0) {
      for (String category : missingDirsCommittedPaths.keySet()) {
        Set<Path> missingPathsPerCategory = missingDirsCommittedPaths.get(category);
        for (Path missingdir : missingPathsPerCategory) {
          if (!fs.exists(missingdir)) {
            LOG.debug("Creating Missing Directory [" + missingdir + "]");
            fs.mkdirs(missingdir);
          }
        }
        prevRuntimeForCategory.put(category, commitTime);
      }
      missingDirsCommittedPaths.clear();
    }
  }

  protected Table<String, Long, Long> parseCounters(CounterGroup counterGrp) {
    Table<String, Long, Long> result = HashBasedTable.create();

    for (Counter counter : counterGrp) {
      String counterName = counter.getName();
      String tmp[] = counterName.split(CopyMapper.DELIMITER);
      if (tmp.length < 2) {
        LOG.error("Malformed counter name,skipping " + counterName);
        continue;
      }
      String filename = tmp[0];
      Long publishTimeWindow = Long.parseLong(tmp[1]);
      Long numOfMsgs = counter.getValue();
      result.put(filename, publishTimeWindow, numOfMsgs);
    }
    return result;

  }

  protected AuditMessage createAuditMessage(String fileName,
      Map<Long, Long> received) {
    String topic = getTopicNameFromFileName(fileName);
    AuditMessage auditMsg = new AuditMessage(new Date().getTime(), topic,
        getTier(),
        hostname, DEFAULT_WINDOW_SIZE, received, null, null, null);
    return auditMsg;
  }

  abstract protected String getTopicNameFromFileName(String fileName);

  abstract protected String getTier();
} 
