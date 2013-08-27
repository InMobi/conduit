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

package org.apache.hadoop.tools.mapred;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.EnumSet;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.tools.DistCpConstants;
import org.apache.hadoop.tools.DistCpOptions.FileAttribute;
import org.apache.hadoop.tools.util.DistCpUtils;
import org.apache.hadoop.tools.util.RetriableCommand;
import org.apache.hadoop.tools.util.ThrottledInputStream;

import com.inmobi.messaging.util.AuditUtil;

/**
 * This class extends RetriableCommand to implement the copy of files,
 * with retries on failure.
 */
public class RetriableFileCopyCommand extends RetriableCommand {

  private static final Log LOG = LogFactory.getLog(RetriableFileCopyCommand.class);
  private static final int BUFFER_SIZE = 8 * 1024;
  private static final int windowSize = 60;
  private CompressionCodecFactory compressionCodecs = null;

  /**
   * Constructor, taking a description of the action.
   * @param description Verbose description of the copy operation.
   */
  public RetriableFileCopyCommand(String description) {
    super(description);
  }

  /**
   * Implementation of RetriableCommand::doExecute().
   * This is the actual copy-implementation.
   * @param arguments Argument-list to the command.
   * @return Number of bytes copied.
   * @throws Exception: CopyReadException, if there are read-failures. All other
   *         failures are IOExceptions.
   */
  @SuppressWarnings("unchecked")
  @Override
  protected Object doExecute(Object... arguments) throws Exception {
    assert arguments.length == 5 : "Unexpected argument list.";
    FileStatus source = (FileStatus)arguments[0];
    assert !source.isDir() : "Unexpected file-status. Expected file.";
    Path target = (Path)arguments[1];
    Mapper.Context context = (Mapper.Context)arguments[2];
    EnumSet<FileAttribute> fileAttributes
            = (EnumSet<FileAttribute>)arguments[3];
    Map<Long, Long> received = (Map<Long, Long>) arguments[4];
    return doCopy(source, target, context, fileAttributes, received);
  }

  private long doCopy(FileStatus sourceFileStatus, Path target,
                      Mapper.Context context,
 EnumSet<FileAttribute> fileAttributes,
      Map<Long, Long> received)
          throws IOException {

    Path tmpTargetPath = getTmpFile(target, context);
    final Configuration configuration = context.getConfiguration();
    FileSystem targetFS = target.getFileSystem(configuration);
    compressionCodecs = new CompressionCodecFactory(context.getConfiguration());
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Copying " + sourceFileStatus.getPath() + " to " + target);
        LOG.debug("Tmp-file path: " + tmpTargetPath);
      }
      FileSystem sourceFS = sourceFileStatus.getPath().getFileSystem(
              configuration);
      long bytesRead = copyToTmpFile(tmpTargetPath, targetFS, sourceFileStatus,
          context, fileAttributes, received);

      compareFileLengths(sourceFileStatus, tmpTargetPath, configuration, bytesRead);
      if (bytesRead > 0) {
        compareCheckSums(sourceFS, sourceFileStatus.getPath(), targetFS, tmpTargetPath);
      }
      promoteTmpToTarget(tmpTargetPath, target, targetFS);
      return bytesRead;

    } finally {
      if (targetFS.exists(tmpTargetPath))
        targetFS.delete(tmpTargetPath, false);
    }
  }

  private long copyToTmpFile(Path tmpTargetPath, FileSystem targetFS,
                             FileStatus sourceFileStatus, Mapper.Context context,
      EnumSet<FileAttribute> fileAttributes, Map<Long, Long> received)
                             throws IOException {
    OutputStream outStream = new BufferedOutputStream(targetFS.create(
            tmpTargetPath, true, BUFFER_SIZE,
            getReplicationFactor(fileAttributes, sourceFileStatus, targetFS),
            getBlockSize(fileAttributes, sourceFileStatus, targetFS), context));
    return copyBytes(sourceFileStatus, outStream, BUFFER_SIZE, true, context,
        received);
  }

  private void compareFileLengths(FileStatus sourceFileStatus, Path target,
                                  Configuration configuration, long bytesRead)
                                  throws IOException {
    final Path sourcePath = sourceFileStatus.getPath();
    FileSystem fs = sourcePath.getFileSystem(configuration);
    if (fs.getFileStatus(sourcePath).getLen() != bytesRead)
      throw new IOException("Mismatch in length of source:" + sourcePath
                + " and target:" + target);
  }

  private void compareCheckSums(FileSystem sourceFS, Path source,
                                FileSystem targetFS, Path target)
                                throws IOException {
    if (!DistCpUtils.checksumsAreEqual(sourceFS, source, targetFS, target))
      throw new IOException("Check-sum mismatch between "
                              + source + " and " + target);

  }

  //If target file exists and unable to delete target - fail
  //If target doesn't exist and unable to create parent folder - fail
  //If target is successfully deleted and parent exists, if rename fails - fail
  private void promoteTmpToTarget(Path tmpTarget, Path target, FileSystem fs)
                                  throws IOException {
    if ((fs.exists(target) && !fs.delete(target, false))
        || (!fs.exists(target.getParent()) && !fs.mkdirs(target.getParent()))
        || !fs.rename(tmpTarget, target)) {
      throw new IOException("Failed to promote tmp-file:" + tmpTarget
                              + " to: " + target);
    }
  }

  private Path getTmpFile(Path target, Mapper.Context context) {
    Path targetWorkPath = new Path(context.getConfiguration().
        get(DistCpConstants.CONF_LABEL_TARGET_WORK_PATH));

    Path root = target.equals(targetWorkPath)? targetWorkPath.getParent() : targetWorkPath;
    LOG.info("Creating temp file: " +
        new Path(root, ".distcp.tmp." + context.getTaskAttemptID().toString()));
    return new Path(root, ".distcp.tmp." + context.getTaskAttemptID().toString());
  }

  private long copyBytes(FileStatus sourceFileStatus, OutputStream outStream,
                         int bufferSize, boolean mustCloseStream,
 Mapper.Context context,
      Map<Long, Long> received) throws IOException {
    Path source = sourceFileStatus.getPath();
    ThrottledInputStream inStream = null;
    long totalBytesRead = 0;
    // GzipCodec gzipCodec = (GzipCodec) ReflectionUtils.newInstance(
    // GzipCodec.class, new Configuration());
    // Decompressor gzipDeCompressor = CodecPool.getDecompressor(gzipCodec);
    // Compressor gzipCompressor = CodecPool.getCompressor(gzipCodec);

    final CompressionCodec codec = compressionCodecs.getCodec(source);
    Text txt = new Text();
    InputStream compressedIn = null;
    OutputStream commpressedOut = null;
    BufferedReader reader = null;

    try {
      inStream = getInputStream(source, context.getConfiguration());
      compressedIn = codec.createInputStream(inStream);
      commpressedOut = codec.createOutputStream(outStream);
      // LineReader reader = new LineReader(compressedIn,
      // context.getConfiguration(), null);
      reader = new BufferedReader(new InputStreamReader(compressedIn));
      byte[] bytesRead = readLine(reader);
      while (bytesRead != null) {
        commpressedOut.write(bytesRead);
        updateContextStatus(totalBytesRead, context, sourceFileStatus);
        byte[] decodedMsg = Base64.decodeBase64(bytesRead);
        if (received != null) {
          incrementReceived(decodedMsg, received);
        }
        bytesRead = readLine(reader);
      }
      context.getCounter(CopyMapper.Counter.SLEEP_TIME_MS).
          increment(inStream.getTotalSleepTime());
      LOG.info("STATS: " + inStream);
    } finally {
      if (mustCloseStream) {
        IOUtils.cleanup(LOG, inStream);
        try {
          if (reader != null)
            reader.close();
          if (compressedIn != null)
            compressedIn.close();
          if (commpressedOut != null)
            commpressedOut.close();
          outStream.close();
        }
        catch(IOException exception) {
          LOG.error("Could not close output-stream. ", exception);
          throw exception;
        }
      }
    }

    return inStream.getTotalBytesRead();// totalBytesRead;
  }

  private static Long getWindow(Long timestamp) {
    Long window = timestamp - (timestamp % (windowSize * 1000));
    return window;
  }

  private static void incrementReceived(byte[] msg, Map<Long, Long> received) {
    long timestamp = AuditUtil.getTimestamp(msg);
    long window = getWindow(timestamp);
    if (timestamp != -1) {
      if (received.containsKey(window)) {
        received.put(window, received.get(timestamp) + 1);
      } else {
        received.put(window, new Long(1));
      }
    }
  }
  private void updateContextStatus(long totalBytesRead, Mapper.Context context,
                                   FileStatus sourceFileStatus) {
    StringBuilder message = new StringBuilder(DistCpUtils.getFormatter()
                .format(totalBytesRead * 100.0f / sourceFileStatus.getLen()));
    message.append("% ")
            .append(description).append(" [")
            .append(DistCpUtils.getStringDescriptionFor(totalBytesRead))
            .append('/')
        .append(DistCpUtils.getStringDescriptionFor(sourceFileStatus.getLen()))
            .append(']');
    context.setStatus(message.toString());
  }

  private static int readBytes(InputStream inStream, byte buf[])
          throws IOException {
    try {
      return inStream.read(buf);
    }
    catch (IOException e) {
      throw new CopyReadException(e);
    }
  }

  private static byte[] readLine(BufferedReader reader)
      throws IOException {
    String line = reader.readLine();

    if (line == null)
      return null;
    return line.getBytes();

  }

  private static ThrottledInputStream getInputStream(Path path, Configuration conf)
          throws IOException {
    try {
      FileSystem fs = path.getFileSystem(conf);
      long bandwidthKB = getAllowedBandwidth(conf);
      return new ThrottledInputStream(new BufferedInputStream(fs.open(path)),
          bandwidthKB * 1024);
    }
    catch (IOException e) {
      throw new CopyReadException(e);
    }
  }

  private static long getAllowedBandwidth(Configuration conf) {
    return (long) conf.getInt(DistCpConstants.CONF_LABEL_BANDWIDTH_KB,
        DistCpConstants.DEFAULT_BANDWIDTH_KB);
  }

    private static short getReplicationFactor(
          EnumSet<FileAttribute> fileAttributes,
          FileStatus sourceFile, FileSystem targetFS) {
    return fileAttributes.contains(FileAttribute.REPLICATION)?
            sourceFile.getReplication() : targetFS.getDefaultReplication();
  }

  private static long getBlockSize(
          EnumSet<FileAttribute> fileAttributes,
          FileStatus sourceFile, FileSystem targetFS) {
    return fileAttributes.contains(FileAttribute.BLOCKSIZE)?
            sourceFile.getBlockSize() : targetFS.getDefaultBlockSize();
  }

  /**
   * Special subclass of IOException. This is used to distinguish read-operation
   * failures from other kinds of IOExceptions.
   * The failure to read from source is dealt with specially, in the CopyMapper.
   * Such failures may be skipped if the DistCpOptions indicate so.
   * Write failures are intolerable, and amount to CopyMapper failure.  
   */
  public static class CopyReadException extends IOException {
    public CopyReadException(Throwable rootCause) {
      super(rootCause);
    }
  }
}

