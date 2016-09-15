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
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.inmobi.conduit.utils.CalendarHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class Cluster {
  @Override
  public String toString() {
    return clustername;
  }

  private final String rootDir;
  private final String hdfsUrl;
  private final String clustername;
  private final String clusterjobqueuename;
  private final Map<String, SourceStream> sourceStreams;
  private final Map<String, DestinationStream> consumeStreams;
//  private final Set<String> sourceStreams;
  private final Configuration hadoopConf;
  private final String copyMapperImpl;
  private final String readUrl;

  public Cluster(Map<String, String> clusterElementsMap, String rootDir,
      Map<String, DestinationStream> consumeStreams, Map<String, SourceStream> sourceStreams)
      throws Exception {
    this.rootDir = rootDir;
    this.clustername = clusterElementsMap.get(ConduitConfigParser.NAME);
    if (clustername == null)
      throw new ParseException(
          "Cluster Name element not found in cluster configuration", 0);
    this.hdfsUrl = clusterElementsMap.get(ConduitConfigParser.HDFS_URL);
    if (hdfsUrl == null)
      throw new ParseException(
          "hdfsurl element not found in cluster configuration " + clustername,
          0);
    this.clusterjobqueuename = clusterElementsMap
        .get(ConduitConfigParser.JOB_QUEUE_NAME);
    if (clusterjobqueuename == null)
      throw new ParseException(
          "Cluster jobqueuename element not found in cluster configuration "
              + clustername, 0);
    if (clusterElementsMap.get(ConduitConfigParser.JT_URL) == null)
      throw new ParseException(
          "jturl element not found in cluster configuration " + clustername, 0);
    this.hadoopConf = new Configuration();
    this.hadoopConf.set("mapred.job.tracker",
        clusterElementsMap.get(ConduitConfigParser.JT_URL));
    this.hadoopConf.set("conduit.tmp.path", getTmpPath().toString());
    this.consumeStreams = consumeStreams;
    this.sourceStreams = sourceStreams;
    this.hadoopConf.set("fs.default.name", hdfsUrl);
    this.hadoopConf.set("mapred.job.queue.name",
    		clusterjobqueuename);
    this.copyMapperImpl = clusterElementsMap.get(ConduitConfigParser
      .COPYMAPPER_IMPL);

    //Sets the readurl which is used in the Mirror amd Merge Stream.
    String tempreadUrl = clusterElementsMap.get(ConduitConfigParser
      .CLUSTER_READ_URL);
    if (tempreadUrl == null) {
      this.readUrl = this.hdfsUrl;
    } else {
      this.readUrl = tempreadUrl;
    }
  }

  public String getRootDir() {
    return hdfsUrl + File.separator + rootDir + File.separator;
  }

  public String getReadUrlRootDir() {
    return getReadUrl() + File.separator + rootDir + File.separator;
  }

  public String getLocalFinalDestDirRoot() {
    String dest = hdfsUrl + File.separator + rootDir + File.separator
        + "streams_local" + File.separator;
    return dest;
  }

	public static String getDateAsYYYYMMDDHHMNPath(long commitTime) {
    Date date = new Date(commitTime);
    return getDateAsYYYYMMDDHHMNPath(date);
  }

	public static String getDateAsYYYYMMDDHHMNPath(Date date) {
    DateFormat dateFormat = new SimpleDateFormat("yyyy" + File.separator + "MM"
        + File.separator + "dd" + File.separator + "HH" + File.separator + "mm"
        + File.separator);
    return dateFormat.format(date);
  }

  public static String getDateAsYYYYMMDDHHPath(long commitTime) {
    Date date = new Date(commitTime);
    DateFormat dateFormat = new SimpleDateFormat("yyyy" + File.separator + "MM"
        + File.separator + "dd" + File.separator + "HH" + File.separator);
    return dateFormat.format(date);
  }

	public static String getDestDir(String destDir, String category,
	    long commitTime) {
		String dest = destDir + File.separator + category + File.separator
		    + getDateAsYYYYMMDDHHMNPath(commitTime);
		return dest;
	}

  public String getLocalDestDir(String category, long commitTime)
      throws IOException {
    Date date = new Date(commitTime);
    return getLocalDestDir(category, date);
  }

  public String getLocalDestDir(String category, Date date) throws IOException {
    String dest = hdfsUrl + File.separator + rootDir + File.separator
        + "streams_local" + File.separator + category + File.separator
        + getDateAsYYYYMMDDHHMNPath(date);
    return dest;
  }

  public synchronized long getCommitTime() {
    Calendar commitTimeMinutes = new GregorianCalendar();
    commitTimeMinutes.set(Calendar.MILLISECOND, 0);
    commitTimeMinutes.set(Calendar.SECOND, 0);
    return commitTimeMinutes.getTimeInMillis();
  }

  public String getHdfsUrl() {
    return hdfsUrl;
  }

  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  public String getName() {
    return clustername;
  }

  public String getUnqaulifiedFinalDestDirRoot() {
    Path absolutePath = new Path(hdfsUrl);
    String dest = File.separator + absolutePath.toUri().getPath() + rootDir
        + File.separator + "streams" + File.separator;
    return dest;
  }

  public String getFinalDestDirRoot() {
    String dest = hdfsUrl + File.separator + rootDir + File.separator
        + "streams" + File.separator;
    return dest;
  }


  public String getReadFinalDestDirRoot() {
    String dest = readUrl + File.separator + rootDir + File.separator
      + "streams" + File.separator;
    return dest;
  }

  public String getDateTimeDestDir(String category, long commitTime) {
    String dest = category + File.separator
        + getDateAsYYYYMMDDHHMNPath(commitTime);
    return dest;
  }

  public String getFinalDestDir(String category, long commitTime)
      throws IOException {
    String dest = hdfsUrl + File.separator + rootDir + File.separator
        + "streams" + File.separator + category + File.separator
        + getDateAsYYYYMMDDHHMNPath(commitTime);
    return dest;
  }

  public String getFinalDestDirTillHour(String category, long commitTime)
      throws IOException {
    String dest = hdfsUrl + File.separator + rootDir + File.separator
        + "streams" + File.separator + category + File.separator
        + getDateAsYYYYMMDDHHPath(commitTime);
    return dest;
  }

  public Map<String, DestinationStream> getDestinationStreams() {
    return consumeStreams;
  }

  public Set<String> getMirroredStreams() {
    Set<String> mirroredStreams = new HashSet<String>();
    for (DestinationStream consumeStream : getDestinationStreams().values()) {
      if (!consumeStream.isPrimary())
        mirroredStreams.add(consumeStream.getName());
    }
    return mirroredStreams;
  }

  public Set<String> getPrimaryDestinationStreams() {
    Set<String> primaryStreams = new HashSet<String>();
    for (DestinationStream consumeStream : getDestinationStreams().values()) {
      if (consumeStream.isPrimary())
        primaryStreams.add(consumeStream.getName());
    }
    return primaryStreams;

  }

  public Set<String> getSourceStreams() {
    return sourceStreams.keySet();
  }

  public Map<String,SourceStream> getSourceStreamsMap() {
    return sourceStreams;
  }

  public Path getTrashPath() {
    return new Path(getSystemDir() + File.separator + "trash");
  }

  public Path getTrashPathWithDate() {
    return new Path(getTrashPath(), CalendarHelper.getCurrentDateAsString());
  }
  
  public Path getTrashPathWithDateHour() {
    return new Path(getTrashPath() + File.separator
        + CalendarHelper.getCurrentDateAsString() + File.separator
        + CalendarHelper.getCurrentHour());
  }

  public Path getDataDir() {
    return new Path(hdfsUrl + File.separator + rootDir + File.separator
        + "data");
  }

  public Path getTmpPath() {
    return new Path(getSystemDir() + File.separator + "tmp");
  }

  public String getCheckpointDir() {
    return getSystemDir() + File.separator + "checkpoint";
  }

  private String getSystemDir() {
    return hdfsUrl + File.separator + rootDir + File.separator + "system";
  }

  public String getJobQueueName() {
    return clusterjobqueuename;
  }

  public String getCopyMapperImpl() {
    return this.copyMapperImpl;
  }

  public String getReadLocalFinalDestDirRoot() {
    String dest = readUrl + File.separator + rootDir + File.separator
      + "streams_local" + File.separator;
    return dest;
  }

  public String getReadUrl() {
    return this.readUrl;
  }

  public String getUnqaulifiedReadUrlFinalDestDirRoot() {
    Path absolutePath = new Path(readUrl);
    String dest = File.separator + absolutePath.toUri().getPath() + rootDir
      + File.separator + "streams" + File.separator;
    return dest;
  }
}
