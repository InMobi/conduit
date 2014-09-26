/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.inmobi.conduit.distcp.tools.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.security.Credentials;

import com.inmobi.conduit.distcp.tools.CopyListing;
import com.inmobi.conduit.distcp.tools.DistCpConstants;
import com.inmobi.conduit.distcp.tools.DistCpOptions;
import com.inmobi.conduit.distcp.tools.GlobbedCopyListing;
import com.inmobi.conduit.distcp.tools.DistCpOptions.FileAttribute;
import com.inmobi.conduit.distcp.tools.util.DistCpUtils;
import com.inmobi.conduit.distcp.tools.util.HadoopCompat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

public class CopyCommitter extends FileOutputCommitter {
  private static final Log LOG = LogFactory.getLog(CopyCommitter.class);

  private final TaskAttemptContext taskAttemptContext;

  /**
   * Create a output committer
   *
   * @param outputPath the job's output path
   * @param context    the task's context
   * @throws IOException - Exception if any
   */
  public CopyCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
    super(outputPath, context);
    this.taskAttemptContext = context;
  }

  /** @inheritDoc */
  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    Configuration conf = HadoopCompat.getConfiguration(jobContext);
    super.commitJob(jobContext);

    cleanupTempFiles(jobContext);

    String attributes = conf.get(DistCpConstants.CONF_LABEL_PRESERVE_STATUS);
    if (attributes != null && !attributes.isEmpty()) {
      preserveFileAttributes(conf);
    }

    if (conf.getBoolean(DistCpConstants.CONF_LABEL_DELETE_MISSING, false)) {
      deleteMissing(conf);
    } else if (conf.getBoolean(DistCpConstants.CONF_LABEL_ATOMIC_COPY, false)) {
      commitData(conf);
    }
    HadoopCompat.setStatus(taskAttemptContext, "Commit Successful");
    cleanup(conf);
  }

  /** @inheritDoc */
  @Override
  public void abortJob(JobContext jobContext,
                       JobStatus.State state) throws IOException {
    try {
      super.abortJob(jobContext, state);
    } finally {
      cleanupTempFiles(jobContext);
      cleanup(HadoopCompat.getConfiguration(jobContext));
    }
  }

  private void cleanupTempFiles(JobContext context) {
    try {
      Configuration conf = HadoopCompat.getConfiguration(context);

      Path targetWorkPath = new Path(conf.get(DistCpConstants.CONF_LABEL_TARGET_WORK_PATH));
      FileSystem targetFS = targetWorkPath.getFileSystem(conf);

      String jobId = HadoopCompat.getJobId(context).toString();
      deleteAttemptTempFiles(targetWorkPath, targetFS, jobId);
      deleteAttemptTempFiles(targetWorkPath.getParent(), targetFS, jobId);
    } catch (Throwable t) {
      LOG.warn("Unable to cleanup temp files", t);
    }
  }

  private void deleteAttemptTempFiles(Path targetWorkPath,
                                      FileSystem targetFS,
                                      String jobId) throws IOException {

    FileStatus[] tempFiles = targetFS.globStatus(
        new Path(targetWorkPath, ".distcp.tmp." + jobId.replaceAll("job","attempt") + "*"));

    if (tempFiles != null && tempFiles.length > 0) {
      for (FileStatus file : tempFiles) {
        LOG.info("Cleaning up " + file.getPath());
        targetFS.delete(file.getPath(), false);
      }
    }
  }

  /**
   * Cleanup meta folder and other temporary files
   *
   * @param conf - Job Configuration
   */
  private void cleanup(Configuration conf) {
    Path metaFolder = new Path(conf.get(DistCpConstants.CONF_LABEL_META_FOLDER));
    try {
      FileSystem fs = metaFolder.getFileSystem(conf);
      LOG.info("Cleaning up temporary work folder: " + metaFolder);
      fs.delete(metaFolder, true);
    } catch (IOException ignore) {
      LOG.error("Exception encountered ", ignore);
    }
  }

  private void preserveFileAttributes(Configuration conf) throws IOException {
    String attrSymbols = conf.get(DistCpConstants.CONF_LABEL_PRESERVE_STATUS);
    LOG.info("About to preserve attributes: " + attrSymbols);

    EnumSet<FileAttribute> attributes = DistCpUtils.unpackAttributes(attrSymbols);

    Path sourceListing = new Path(conf.get(DistCpConstants.CONF_LABEL_LISTING_FILE_PATH));
    FileSystem clusterFS = sourceListing.getFileSystem(conf);
    SequenceFile.Reader sourceReader = new SequenceFile.Reader(clusterFS, sourceListing, conf);
    long totalLen = clusterFS.getFileStatus(sourceListing).getLen();

    Path targetRoot = new Path(conf.get(DistCpConstants.CONF_LABEL_TARGET_WORK_PATH));

    long preservedEntries = 0;
    try {
      FileStatus srcFileStatus = new FileStatus();
      Text srcRelPath = new Text();

      while (sourceReader.next(srcRelPath, srcFileStatus)) {
        if (! srcFileStatus.isDir()) continue;

        Path targetFile = new Path(targetRoot.toString() + "/" + srcRelPath);

        //Skip the root folder, preserve the status after atomic commit is complete
        //If it is changed any earlier, then atomic commit may fail
        if (targetRoot.equals(targetFile)) continue;

        FileSystem targetFS = targetFile.getFileSystem(conf);
        DistCpUtils.preserve(targetFS, targetFile, srcFileStatus,  attributes);

        HadoopCompat.progress(taskAttemptContext);
        HadoopCompat.setStatus(taskAttemptContext, "Preserving status on directory entries. [" +
          sourceReader.getPosition() * 100 / totalLen + "%]");
      }
    } finally {
      IOUtils.closeStream(sourceReader);
    }
    LOG.info("Preserved status on " + preservedEntries + " dir entries on target");
  }

  private void deleteMissing(Configuration conf) throws IOException {
    LOG.info("-delete option is enabled. About to remove entries from " +
        "target that are missing in source");

    Path sourceListing = new Path(conf.get(DistCpConstants.CONF_LABEL_LISTING_FILE_PATH));
    FileSystem clusterFS = sourceListing.getFileSystem(conf);
    Path sortedSourceListing = DistCpUtils.sortListing(clusterFS, conf, sourceListing);

    Path targetListing = new Path(sourceListing.getParent(), "targetListing.seq");
    CopyListing target = new GlobbedCopyListing(conf, null);

    List<Path> targets = new ArrayList<Path>(1);
    Path targetFinalPath = new Path(conf.get(DistCpConstants.CONF_LABEL_TARGET_FINAL_PATH));
    targets.add(targetFinalPath);
    DistCpOptions options = new DistCpOptions(targets, new Path("/NONE"));

    target.buildListing(targetListing, options);
    Path sortedTargetListing = DistCpUtils.sortListing(clusterFS, conf, targetListing);
    long totalLen = clusterFS.getFileStatus(sortedTargetListing).getLen();

    SequenceFile.Reader sourceReader = new SequenceFile.Reader(clusterFS, sortedSourceListing, conf);
    SequenceFile.Reader targetReader = new SequenceFile.Reader(clusterFS, sortedTargetListing, conf);

    long deletedEntries = 0;
    try {
      FileStatus srcFileStatus = new FileStatus();
      Text srcRelPath = new Text();
      FileStatus trgtFileStatus = new FileStatus();
      Text trgtRelPath = new Text();

      FileSystem targetFS = targetFinalPath.getFileSystem(conf);
      boolean srcAvailable = sourceReader.next(srcRelPath, srcFileStatus);
      while (targetReader.next(trgtRelPath, trgtFileStatus)) {
        while (srcAvailable && trgtRelPath.compareTo(srcRelPath) > 0) {
          srcAvailable = sourceReader.next(srcRelPath, srcFileStatus);
        }

        if (srcAvailable && trgtRelPath.equals(srcRelPath)) continue;

        boolean result = (!targetFS.exists(trgtFileStatus.getPath()) ||
            targetFS.delete(trgtFileStatus.getPath(), true));
        if (result) {
          LOG.info("Deleted " + trgtFileStatus.getPath() + " - Missing at source");
          deletedEntries++;
        } else {
          throw new IOException("Unable to delete " + trgtFileStatus.getPath());
        }
        HadoopCompat.progress(taskAttemptContext);
        HadoopCompat.setStatus(taskAttemptContext, "Deleting missing files from target. [" +
          targetReader.getPosition() * 100 / totalLen + "%]");
      }
    } finally {
      IOUtils.closeStream(sourceReader);
      IOUtils.closeStream(targetReader);
    }
    LOG.info("Deleted " + deletedEntries + " from target: " + targets.get(0));
  }

  private void commitData(Configuration conf) throws IOException {

    Path workDir = new Path(conf.get(DistCpConstants.CONF_LABEL_TARGET_WORK_PATH));
    Path finalDir = new Path(conf.get(DistCpConstants.CONF_LABEL_TARGET_FINAL_PATH));
    FileSystem targetFS = workDir.getFileSystem(conf);

    LOG.info("Atomic commit enabled. Moving " + workDir + " to " + finalDir);
    if (targetFS.exists(finalDir) && targetFS.exists(workDir))
      if (!targetFS.delete(finalDir, true)) {
        LOG.error("Unable to delete pre-existing final-data at " + finalDir);
        throw new IOException("Atomic commit failed. Pre-existing final data" +
                " in " + finalDir + " could not be cleared, before commit.");
      }

    boolean result = targetFS.rename(workDir, finalDir);
    if (!result) {
      LOG.warn("Rename failed. Perhaps data already moved. Verifying...");
      result = targetFS.exists(finalDir) && !targetFS.exists(workDir);
    }
    if (result) {
      LOG.info("Data committed successfully to " + finalDir);
      HadoopCompat.setStatus(taskAttemptContext,"Data committed successfully to " + finalDir );
    } else {
      LOG.error("Unable to commit data to " + finalDir);
      throw new IOException("Atomic commit failed. Temporary data in " + workDir +
        ", Unable to move to " + finalDir);
    }
  }
}
