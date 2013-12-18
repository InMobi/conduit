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

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.util.TestDistCpUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.inmobi.conduit.DatabusConstants;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.util.AuditUtil;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Date;
import java.util.Scanner;

public class TestIntegration {
  private static final Log LOG = LogFactory.getLog(TestIntegration.class);

  private static FileSystem fs;

  private static Path listFile;
  private static Path target;
  private static String root;
  private static final Path counterOutputPath = new Path("counters");

  private static Configuration getConf() {
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "file:///");
    conf.set("mapred.job.tracker", "local");
    conf.set("mapred.num.entries.per.chunk", "1");   // Needed, because conf isn't copied when using LocalJobRunner.
    return conf;
  }

  @BeforeClass
  public static void setup() {
    try {
      fs = FileSystem.get(getConf());
      listFile = new Path("target/tmp/listing").makeQualified(fs);
      target = new Path("target/tmp/target").makeQualified(fs);
      root = new Path("target/tmp").makeQualified(fs).toString();
      TestDistCpUtils.delete(fs, root);
    } catch (IOException e) {
      LOG.error("Exception encountered ", e);
    }
  }

  @Test
  public void testJobConters() {
    try {
      Path listFile = new Path("target/tmp1/listing").makeQualified(fs);
      addEntries(listFile, "*");
      createFileForAudit("/databus/streams/test1/2013/10/10/10/10/file1.gz");
      runTest(listFile, target, true);
      int numberOfCountersPerFile = 0;
      long sumOfCounterValues = 0;
      FileStatus[] statuses = fs.listStatus(counterOutputPath, new PathFilter() {
        public boolean accept(Path path) {
          return path.toString().contains("part");
        }
      });
      for (FileStatus status : statuses) {
        Scanner scanner = new Scanner(fs.open(status.getPath()));
        while (scanner.hasNext()) {
          String counterNameValue = null;
          try {
            counterNameValue = scanner.next();
            String tmp[] = counterNameValue.split(DatabusConstants.AUDIT_COUNTER_NAME_DELIMITER);
            Assert.assertEquals(4, tmp.length);
            Long numOfMsgs = Long.parseLong(tmp[3]);
            numberOfCountersPerFile++;
            sumOfCounterValues += numOfMsgs;
          } catch (Exception e) {
            LOG.error("Counters file has malformed line with counter name = "
                + counterNameValue + " ..skipping the line ", e);
          }
        }
      }
      // should have 2 conters per file
      Assert.assertEquals(2, numberOfCountersPerFile);
      // sum of all counter values should equal to total number of messages
      Assert.assertEquals(3, sumOfCounterValues);
      checkResult(target, 1);
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  private void createFileForAudit(String... entries) throws IOException {
    for (String entry : entries) {
      OutputStream out = fs.create(new Path(root + "/" + entry));
      GzipCodec gzipCodec = ReflectionUtils.newInstance(
          GzipCodec.class, new Configuration());
      Compressor gzipCompressor = CodecPool.getCompressor(gzipCodec);
      OutputStream compressedOut =null;
      try {
        compressedOut = gzipCodec.createOutputStream(out, gzipCompressor);
        Message msg = new Message((root + "/" + entry).getBytes());
        long currentTimestamp = new Date().getTime();
        AuditUtil.attachHeaders(msg, currentTimestamp);
        byte[] encodeMsg = Base64.encodeBase64(msg.getData().array());
        compressedOut.write(encodeMsg);
        compressedOut.write("\n".getBytes());
        compressedOut.write(encodeMsg);
        compressedOut.write("\n".getBytes());
        /* add a different timestamp to the msg header.
         * The generation time stamp falls in diff window
         * Each file will be having two counters
         */
        AuditUtil.attachHeaders(msg, currentTimestamp + 60001);
        encodeMsg = Base64.encodeBase64(msg.getData().array());
        compressedOut.write(encodeMsg);
        compressedOut.write("\n".getBytes());
        compressedOut.flush();
      } finally {
        compressedOut.close();
        out.close();
        CodecPool.returnCompressor(gzipCompressor);
      }
    }
  }

  @Test
  public void testSingleFileMissingTarget() {
    caseSingleFileMissingTarget(false);
    caseSingleFileMissingTarget(true);
  }

  private void caseSingleFileMissingTarget(boolean sync) {

    try {
      addEntries(listFile, "singlefile1/file1.gz");
      createFiles("singlefile1/file1.gz");

      runTest(listFile, target, sync);

      checkResult(target, 1);
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test
  public void testSingleFileTargetFile() {
    caseSingleFileTargetFile(false);
    caseSingleFileTargetFile(true);
  }

  private void caseSingleFileTargetFile(boolean sync) {

    try {
      addEntries(listFile, "singlefile1/file1.gz");
      createFiles("singlefile1/file1.gz", target.toString());

      runTest(listFile, target, sync);

      checkResult(target, 1);
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test
  public void testSingleFileTargetDir() {
    caseSingleFileTargetDir(false);
    caseSingleFileTargetDir(true);
  }

  private void caseSingleFileTargetDir(boolean sync) {

    try {
      addEntries(listFile, "singlefile2/file2.gz");
      createFiles("singlefile2/file2.gz");
      mkdirs(target.toString());

      runTest(listFile, target, sync);

      checkResult(target, 1, "file2.gz");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test
  public void testSingleDirTargetMissing() {
    caseSingleDirTargetMissing(false);
    caseSingleDirTargetMissing(true);
  }

  private void caseSingleDirTargetMissing(boolean sync) {

    try {
      addEntries(listFile, "singledir");
      mkdirs(root + "/singledir/dir1");

      runTest(listFile, target, sync);

      checkResult(target, 1, "dir1");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test
  public void testSingleDirTargetPresent() {

    try {
      addEntries(listFile, "singledir");
      mkdirs(root + "/singledir/dir1");
      mkdirs(target.toString());

      runTest(listFile, target, false);

      checkResult(target, 1, "singledir/dir1");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test
  public void testUpdateSingleDirTargetPresent() {

    try {
      addEntries(listFile, "Usingledir");
      mkdirs(root + "/Usingledir/Udir1");
      mkdirs(target.toString());

      runTest(listFile, target, true);

      checkResult(target, 1, "Udir1");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test
  public void testMultiFileTargetPresent() {
    caseMultiFileTargetPresent(false);
    caseMultiFileTargetPresent(true);
  }

  private void caseMultiFileTargetPresent(boolean sync) {

    try {
      addEntries(listFile, "multifile/file3.gz", "multifile/file4.gz", "multifile/file5.gz");
      createFiles("multifile/file3.gz", "multifile/file4.gz", "multifile/file5.gz");
      mkdirs(target.toString());

      runTest(listFile, target, sync);

      checkResult(target, 3, "file3.gz", "file4.gz", "file5.gz");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test
  public void testMultiFileTargetMissing() {
    caseMultiFileTargetMissing(false);
    caseMultiFileTargetMissing(true);
  }

  private void caseMultiFileTargetMissing(boolean sync) {

    try {
      addEntries(listFile, "multifile/file3.gz", "multifile/file4.gz", "multifile/file5.gz");
      createFiles("multifile/file3.gz", "multifile/file4.gz", "multifile/file5.gz");

      runTest(listFile, target, sync);

      checkResult(target, 3, "file3.gz", "file4.gz", "file5.gz");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test
  public void testMultiDirTargetPresent() {

    try {
      addEntries(listFile, "multifile", "singledir");
      createFiles("multifile/file3.gz", "multifile/file4.gz", "multifile/file5.gz");
      mkdirs(target.toString(), root + "/singledir/dir1");

      runTest(listFile, target, false);

      checkResult(target, 2, "multifile/file3.gz", "multifile/file4.gz", "multifile/file5.gz", "singledir/dir1");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test
  public void testUpdateMultiDirTargetPresent() {

    try {
      addEntries(listFile, "Umultifile", "Usingledir");
      createFiles("Umultifile/Ufile3.gz", "Umultifile/Ufile4.gz", "Umultifile/Ufile5.gz");
      mkdirs(target.toString(), root + "/Usingledir/Udir1");

      runTest(listFile, target, true);

      checkResult(target, 4, "Ufile3.gz", "Ufile4.gz", "Ufile5.gz", "Udir1");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test
  public void testMultiDirTargetMissing() {

    try {
      addEntries(listFile, "multifile", "singledir");
      createFiles("multifile/file3.gz", "multifile/file4.gz", "multifile/file5.gz");
      mkdirs(root + "/singledir/dir1");

      runTest(listFile, target, false);

      checkResult(target, 2, "multifile/file3.gz", "multifile/file4.gz",
          "multifile/file5.gz", "singledir/dir1");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test
  public void testUpdateMultiDirTargetMissing() {

    try {
      addEntries(listFile, "multifile", "singledir");
      createFiles("multifile/file3.gz", "multifile/file4.gz", "multifile/file5.gz");
      mkdirs(root + "/singledir/dir1");

      runTest(listFile, target, true);

      checkResult(target, 4, "file3.gz", "file4.gz", "file5.gz", "dir1");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test
  public void testGlobTargetMissingSingleLevel() {

    try {
      Path listFile = new Path("target/tmp1/listing").makeQualified(fs);
      addEntries(listFile, "*");
      createFiles("multifile/streams/test1/file3.gz",
          "multifile/streams/test1/file4.gz", "multifile/streams/test1/file5.gz");
      createFiles("singledir/dir2/streams/test1/file6.gz");

      runTest(listFile, target, false);

      checkResult(target, 2, "multifile/streams/test1/file3.gz",
          "multifile/streams/test1/file4.gz", "multifile/streams/test1/file5.gz",
          "singledir/dir2/streams/test1/file6.gz");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
      TestDistCpUtils.delete(fs, "target/tmp1");
    }
  }

  @Test
  public void testUpdateGlobTargetMissingSingleLevel() {

    try {
      Path listFile = new Path("target/tmp1/listing").makeQualified(fs);
      addEntries(listFile, "*");
      createFiles("multifile/file3.gz", "multifile/file4.gz", "multifile/file5.gz");
      createFiles("singledir/dir2/file6.gz");

      runTest(listFile, target, true);

      checkResult(target, 4, "file3.gz", "file4.gz", "file5.gz", "dir2/file6.gz");
    } catch (IOException e) {
      LOG.error("Exception encountered while running distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
      TestDistCpUtils.delete(fs, "target/tmp1");
    }
  }

  @Test
  public void testGlobTargetMissingMultiLevel() {

    try {
      Path listFile = new Path("target/tmp1/listing").makeQualified(fs);
      addEntries(listFile, "*/*");
      createFiles("multifile/file3.gz", "multifile/file4.gz", "multifile/file5.gz");
      createFiles("singledir1/dir3/file7.gz", "singledir1/dir3/file8.gz",
          "singledir1/dir3/file9.gz");

      runTest(listFile, target, false);

      checkResult(target, 4, "file3.gz", "file4.gz", "file5.gz",
          "dir3/file7.gz", "dir3/file8.gz", "dir3/file9.gz");
    } catch (IOException e) {
      LOG.error("Exception encountered while running distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
      TestDistCpUtils.delete(fs, "target/tmp1");
    }
  }

  @Test
  public void testUpdateGlobTargetMissingMultiLevel() {

    try {
      Path listFile = new Path("target/tmp1/listing").makeQualified(fs);
      addEntries(listFile, "*/*");
      createFiles("multifile/file3.gz", "multifile/file4.gz", "multifile/file5.gz");
      createFiles("singledir1/dir3/file7.gz", "singledir1/dir3/file8.gz",
          "singledir1/dir3/file9.gz");

      runTest(listFile, target, true);

      checkResult(target, 6, "file3.gz", "file4.gz", "file5.gz",
          "file7.gz", "file8.gz", "file9.gz");
    } catch (IOException e) {
      LOG.error("Exception encountered while running distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
      TestDistCpUtils.delete(fs, "target/tmp1");
    }
  }

  private void addEntries(Path listFile, String... entries) throws IOException {
    OutputStream out = fs.create(listFile);
    try {
      for (String entry : entries){
        out.write((root + "/" + entry).getBytes());
        out.write("\n".getBytes());
      }
    } finally {
      out.close();
    }
  }

  private void createFiles(String... entries) throws IOException {
    for (String entry : entries){
      OutputStream out = fs.create(new Path(root + "/" + entry));
      GzipCodec gzipCodec = ReflectionUtils.newInstance(
          GzipCodec.class, new Configuration());
      Compressor gzipCompressor = CodecPool.getCompressor(gzipCodec);
      OutputStream compressedOut =null;
      try {
        compressedOut = gzipCodec.createOutputStream(out, gzipCompressor);
        compressedOut.write((root + "/" + entry).getBytes());
        compressedOut.write("\n".getBytes());
      } finally {
        compressedOut.close();
        out.close();
        CodecPool.returnCompressor(gzipCompressor);
      }
    }
  }

  private void mkdirs(String... entries) throws IOException {
    for (String entry : entries){
      fs.mkdirs(new Path(entry));
    }
  }

  private Job runTest(Path listFile, Path target, boolean sync) throws IOException {
    DistCpOptions options = new DistCpOptions(listFile, target);
    options.setOutPutDirectory(counterOutputPath);
    options.setSyncFolder(sync);
    try {
      Job job = new DistCp(getConf(), options).execute();
      return job;
    } catch (Exception e) {
      LOG.error("Exception encountered ", e);
      throw new IOException(e);
    }
  }

  private void checkResult(Path target, int count, String... relPaths) throws IOException {
    Assert.assertEquals(count, fs.listStatus(target).length);
    if (relPaths == null || relPaths.length == 0) {
      Assert.assertTrue(target.toString(), fs.exists(target));
      return;
    }
    for (String relPath : relPaths) {
      Assert.assertTrue(new Path(target, relPath).toString(), fs.exists(new Path(target, relPath)));
    }
  }

}
