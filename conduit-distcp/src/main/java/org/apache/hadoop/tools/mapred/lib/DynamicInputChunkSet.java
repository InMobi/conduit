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

package org.apache.hadoop.tools.mapred.lib;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.tools.DistCpConstants;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.util.HadoopCompat;

import java.io.IOException;

/**
 * The DynamicInputChunkSet abstracts the context in which a DynamicInputChunk
 * is constructed, acquired or released.
 * There is one instance of DynamicInputChunkSet for each unique Hadoop job
 * that uses the DynamicInputFormat.
 */
public class DynamicInputChunkSet {
  private static Log LOG = LogFactory.getLog(DynamicInputChunkSet.class);

  private Configuration configuration;
  private Path chunkRootPath;
  private String chunkFilePrefix;
  private int numChunksLeft = -1; // Un-initialized before 1st dir-scan.
  private FileSystem fs;

  /**
   * Constructor, to initialize the context in which DynamicInputChunks are
   * used.
   * @param configuration The Configuration instance, as received from the
   * DynamicInputFormat or DynamicRecordReader.
   * @throws IOException Exception in case of failure.
   */
  public DynamicInputChunkSet(Configuration configuration) throws IOException {
    this.configuration = configuration;
    Path listingFilePath = new Path(getListingFilePath(configuration));
    chunkRootPath = new Path(listingFilePath.getParent(), "chunkDir");
    fs = chunkRootPath.getFileSystem(configuration);
    chunkFilePrefix = listingFilePath.getName() + ".chunk.";
  }

  /**
   * Getter for the Configuration with which the DynamicInputChunkSet was
   * constructed.
   * @return Configuration object.
   */
  public Configuration getConf() {
    return configuration;
  }

  /**
   * The root-path of the directory where DynamicInputChunks are stored.
   * @return The chunk-directory location.
   */
  public Path getChunkRootPath() {
    return chunkRootPath;
  }

  /**
   * The string with which all chunk-file-names are prefixed.
   * @return Prefix string, for all chunk files.
   */
  public String getChunkFilePrefix() {
    return chunkFilePrefix;
  }

  /**
   * Number of chunk-files left, on last directory scan.
   * @return If chunk-directory hasn't been scanned yet, -1. Otherwise, the
   * number of chunk-files left.
   */
  public int getNumChunksLeft() {
    return numChunksLeft;
  }

  /**
   * FileSystem instance, for the file-system where the chunk-files are stored.
   * @return FileSystem instance.
   */
  public FileSystem getFileSystem() {
    return fs;
  }

  private static String getListingFilePath(Configuration configuration) {
    final String listingFileString = configuration.get(
            DistCpConstants.CONF_LABEL_LISTING_FILE_PATH, "");
    assert !listingFileString.equals("") : "Listing file not found.";
    return listingFileString;
  }

  /**
   * Factory method to create chunk-files for writing to.
   * (For instance, when the DynamicInputFormat splits the input-file into
   * chunks.)
   * @param chunkId String to identify the chunk.
   * @return A DynamicInputChunk, corresponding to a chunk-file, with the name
   * incorporating the chunk-id.
   * @throws IOException Exception on failure to create the chunk.
   */
  public DynamicInputChunk createChunkForWrite(String chunkId) throws IOException {
    return new DynamicInputChunk(chunkId, this);
  }

  /**
   * Factory method that
   * 1. acquires a chunk for the specified map-task attempt
   * 2. returns a DynamicInputChunk associated with the acquired chunk-file.
   * @param taskAttemptContext The attempt-context for the map task that's
   * trying to acquire a chunk.
   * @return The acquired dynamic-chunk. The chunk-file is renamed to the
   * attempt-id (from the attempt-context.)
   * @throws IOException Exception on failure.
   * @throws InterruptedException Exception on failure.
   */
  public DynamicInputChunk acquire(TaskAttemptContext taskAttemptContext)
                                      throws IOException, InterruptedException {

    String taskId
            = HadoopCompat.getTaskAttemptID(taskAttemptContext).getTaskID().toString();
    Path acquiredFilePath = new Path(chunkRootPath, taskId);

    if (fs.exists(acquiredFilePath)) {
      LOG.info("Acquiring pre-assigned chunk: " + acquiredFilePath);
      return new DynamicInputChunk(acquiredFilePath, taskAttemptContext);
    }

    for (FileStatus chunkFile : getListOfChunkFiles()) {
      if (fs.rename(chunkFile.getPath(), acquiredFilePath)) {
        LOG.info(taskId + " acquired " + chunkFile.getPath());
        return new DynamicInputChunk(acquiredFilePath, taskAttemptContext);
      }
      else
        LOG.warn(taskId + " could not acquire " + chunkFile.getPath());
    }

    return null;
  }

  FileStatus [] getListOfChunkFiles() throws IOException {
    Path chunkFilePattern = new Path(chunkRootPath, chunkFilePrefix + "*");
    FileStatus chunkFiles[] = fs.globStatus(chunkFilePattern);
    numChunksLeft = chunkFiles.length;
    return chunkFiles;
  }
}
