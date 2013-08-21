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

package org.apache.hadoop.tools;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.tools.util.DistCpUtils;

public class SimpleCopyListing extends CopyListing {
  private static final Log LOG = LogFactory.getLog(SimpleCopyListing.class);

  private long totalPaths = 0;
  private long totalBytesToCopy = 0;

  /**
   * Protected constructor, to initialize configuration.
   *
   * @param configuration: The input configuration, with which the source/target FileSystems may be accessed.
   * @param credentials - Credentials object on which the FS delegation tokens are cached. If null
   * delegation token caching is skipped
   */
  protected SimpleCopyListing(Configuration configuration, Credentials credentials) {
    super(configuration, credentials);
  }

  @Override
  protected void validatePaths(DistCpOptions options)
      throws IOException, InvalidInputException {

    if (options.isSkipPathValidation()) {
      LOG.debug("Skipping Path Validation in disctp");
      return;
    }

    Path targetPath = options.getTargetPath();
    FileSystem targetFS = targetPath.getFileSystem(getConf());
    boolean targetIsFile = targetFS.isFile(targetPath);

    //If target is a file, then source has to be single file
    if (targetIsFile) {
      if (options.getSourcePaths().size() > 1) {
        throw new InvalidInputException("Multiple source being copied to a file: " +
            targetPath);
      }

      Path srcPath = options.getSourcePaths().get(0);
      FileSystem sourceFS = srcPath.getFileSystem(getConf());
      if (!sourceFS.isFile(srcPath)) {
        throw new InvalidInputException("Cannot copy " + srcPath +
            ", which is not a file to " + targetPath);
      }
    }

    for (Path path: options.getSourcePaths()) {
      FileSystem fs = path.getFileSystem(getConf());
      if (!fs.exists(path)) {
        throw new InvalidInputException(path + " doesn't exist");
      }
    }

    /* This is requires to allow map tasks to access each of the source
       clusters. This would retrieve the delegation token for each unique
       file system and add them to job's private credential store
     */
    Credentials credentials = getCredentials();
    if (credentials != null) {
      Path[] inputPaths = options.getSourcePaths().toArray(new Path[1]);
      TokenCache.obtainTokensForNamenodes(credentials, inputPaths, getConf());
    }
  }

  /** {@inheritDoc} */
  @Override
  public void doBuildListing(Path pathToListingFile, DistCpOptions options) throws IOException {

    SequenceFile.Writer fileListWriter = null;

    try {
      fileListWriter = getWriter(pathToListingFile);

      for (Path path: options.getSourcePaths()) {
        FileSystem sourceFS = path.getFileSystem(getConf());
        path = makeQualified(path);

        FileStatus rootStatus = sourceFS.getFileStatus(path);
        Path sourcePathRoot = computeSourceRootPath(rootStatus, options);
        boolean localFile = (rootStatus.getClass() != FileStatus.class);

        FileStatus[] sourceFiles = sourceFS.listStatus(path);
        if (sourceFiles != null && sourceFiles.length > 0) {
          for (FileStatus sourceStatus: sourceFiles) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Recording source-path: " + sourceStatus.getPath() + " for copy.");
            }
            writeToFileListing(fileListWriter, sourceStatus, sourcePathRoot, localFile, options);

            if (isDirectoryAndNotEmpty(sourceFS, sourceStatus)) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Traversing non-empty source dir: " + sourceStatus.getPath());
              }
              traverseNonEmptyDirectory(fileListWriter, sourceStatus,
                  sourcePathRoot, localFile, options);
            }
          }
        } else {
          writeToFileListing(fileListWriter, rootStatus, sourcePathRoot, localFile, options);
        }
      }
    } finally {
      try {
        if (fileListWriter != null) fileListWriter.close();
      }
      catch(IOException exception) {
        LOG.error("Could not close output-steam to the file-list: ", exception);
        throw exception;
      }
    }
  }

  private Path computeSourceRootPath(FileStatus sourceStatus,
                                     DistCpOptions options) throws IOException {

    Path target = options.getTargetPath();
    FileSystem targetFS = target.getFileSystem(getConf());

    boolean solitaryFile = options.getSourcePaths().size() == 1 && !sourceStatus.isDir();

    if (solitaryFile) {
      if (targetFS.isFile(target) || !targetFS.exists(target)) {
        return sourceStatus.getPath();
      } else {
        return sourceStatus.getPath().getParent();
      }
    } else {
      boolean specialHandling = (options.getSourcePaths().size() == 1 && !targetFS.exists(target)) ||
          options.shouldSyncFolder() || options.shouldOverwrite();

      return specialHandling && sourceStatus.isDir() ? sourceStatus.getPath() :
          sourceStatus.getPath().getParent();
    }
  }

  /** {@inheritDoc} */
  @Override
  protected long getBytesToCopy() {
    return totalBytesToCopy;
  }

  /** {@inheritDoc} */
  @Override
  protected long getNumberOfPaths() {
    return totalPaths;
  }

  protected boolean shouldCopy(Path path, DistCpOptions options) {
    return true;
  }

  private Path makeQualified(Path path) throws IOException {
    return path.makeQualified(path.getFileSystem(getConf()));
  }

  private SequenceFile.Writer getWriter(Path pathToListFile) throws IOException {
    return SequenceFile.createWriter(pathToListFile.getFileSystem(getConf()),
        getConf(), pathToListFile,
        Text.class, FileStatus.class, SequenceFile.CompressionType.NONE);
  }

  private static boolean isDirectoryAndNotEmpty(FileSystem fileSystem,
                                                FileStatus fileStatus) throws IOException {
    return fileStatus.isDir() && getChildren(fileSystem, fileStatus).length > 0;
  }

  private static FileStatus[] getChildren(FileSystem fileSystem,
                                          FileStatus parent) throws IOException {
    return fileSystem.listStatus(parent.getPath());
  }

  private void traverseNonEmptyDirectory(SequenceFile.Writer fileListWriter,
                                         FileStatus sourceStatus,
                                         Path sourcePathRoot, boolean localFile,
                                         DistCpOptions options)
      throws IOException {
    FileSystem sourceFS = sourcePathRoot.getFileSystem(getConf());
    Stack<FileStatus> pathStack = new Stack<FileStatus>();
    pathStack.push(sourceStatus);

    while (!pathStack.isEmpty()) {
      for (FileStatus child: getChildren(sourceFS, pathStack.pop())) {
        if (LOG.isDebugEnabled())
          LOG.debug("Recording source-path: "
              + sourceStatus.getPath() + " for copy.");
        writeToFileListing(fileListWriter, child, sourcePathRoot, localFile,
            options);
        if (isDirectoryAndNotEmpty(sourceFS, child)) {
          if (LOG.isDebugEnabled())
            LOG.debug("Traversing non-empty source dir: "
                + sourceStatus.getPath());
          pathStack.push(child);
        }
      }
    }
  }

  private void writeToFileListing(SequenceFile.Writer fileListWriter,
                                  FileStatus fileStatus, Path sourcePathRoot,
                                  boolean localFile, DistCpOptions options)
      throws IOException {
    if (fileStatus.getPath().equals(sourcePathRoot) && fileStatus.isDir())
      return; // Skip the root-paths.

    if (LOG.isDebugEnabled()) {
      LOG.debug("REL PATH: " + DistCpUtils.getRelativePath(sourcePathRoot,
          fileStatus.getPath()) + ", FULL PATH: " + fileStatus.getPath());
    }

    FileStatus status = fileStatus;
    if (localFile) {
      status = getFileStatus(fileStatus);
    }

    if (!shouldCopy(fileStatus.getPath(), options)) return;
    if (options.shouldPreserveSrcPath()) {
      fileListWriter.append(new Text(fileStatus.getPath().toUri().getPath()), status);
    } else {
      fileListWriter.append(new Text(DistCpUtils.getRelativePath(sourcePathRoot,
          fileStatus.getPath())), status);
    }

    fileListWriter.sync();

    if (!fileStatus.isDir()) {
      totalBytesToCopy += fileStatus.getLen();
    }
    totalPaths++;
  }

  private final ByteArrayOutputStream buffer = new ByteArrayOutputStream(64);
  private DataInputBuffer in = new DataInputBuffer();

  private FileStatus getFileStatus(FileStatus fileStatus) throws IOException {
    FileStatus status = new FileStatus();

    buffer.reset();
    DataOutputStream out = new DataOutputStream(buffer);
    fileStatus.write(out);

    in.reset(buffer.toByteArray(), 0, buffer.size());
    status.readFields(in);
    return status;
  }
}
