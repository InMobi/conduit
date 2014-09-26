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

package com.inmobi.conduit.distcp.tools;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.Assert;
import org.junit.AfterClass;

import com.inmobi.conduit.distcp.tools.DistCp;
import com.inmobi.conduit.distcp.tools.DistCpConstants;
import com.inmobi.conduit.distcp.tools.DistCpOptions;
import com.inmobi.conduit.distcp.tools.mapred.CopyOutputFormat;

import java.util.List;
import java.util.ArrayList;
import java.io.*;

public class TestDistCp {
  private static final Log LOG = LogFactory.getLog(TestDistCp.class);
  private static List<Path> pathList = new ArrayList<Path>();
  private static final int FILE_SIZE = 1024;

  private static Configuration configuration;
  private static MiniDFSCluster cluster;
  private static MiniMRCluster mrCluster;

  private static final String SOURCE_PATH = "/tmp/source";
  private static final String TARGET_PATH = "/tmp/target";
  private static final Path counterOutputPath = new Path("counters");

  @BeforeClass
  public static void setup() throws Exception {
    configuration = getConfigurationForCluster();
    cluster = new MiniDFSCluster(configuration, 1, true, null);
    System.setProperty("org.apache.hadoop.mapred.TaskTracker", "target/tmp");
    configuration.set("org.apache.hadoop.mapred.TaskTracker", "target/tmp");
    System.setProperty("hadoop.log.dir", "target/tmp");
    configuration.set("hadoop.log.dir", "target/tmp");
    mrCluster = new MiniMRCluster(1, configuration.get("fs.default.name"), 1);
    Configuration mrConf = mrCluster.createJobConf();
    final String mrJobTracker = mrConf.get("mapred.job.tracker");
    configuration.set("mapred.job.tracker", mrJobTracker);
    final String mrJobTrackerAddress
            = mrConf.get("mapred.job.tracker.http.address");
    configuration.set("mapred.job.tracker.http.address", mrJobTrackerAddress);
  }

  @AfterClass
  public static void cleanup() {
    if (mrCluster != null) mrCluster.shutdown();
    if (cluster != null) cluster.shutdown();
  }

  private static Configuration getConfigurationForCluster() throws IOException {
    Configuration configuration = new Configuration();
    System.setProperty("test.build.data", "target/build/TEST_DISTCP/data");
    configuration.set("hadoop.log.dir", "target/tmp");

    LOG.debug("fs.default.name  == " + configuration.get("fs.default.name"));
    LOG.debug("dfs.http.address == " + configuration.get("dfs.http.address"));
    return configuration;
  }

  private static void createSourceData() throws Exception {
    mkdirs(SOURCE_PATH + "/1");
    mkdirs(SOURCE_PATH + "/2");
    mkdirs(SOURCE_PATH + "/2/3/4.gz");
    mkdirs(SOURCE_PATH + "/2/3");
    mkdirs(SOURCE_PATH + "/5");
    touchFile(SOURCE_PATH + "/5/6.gz");
    mkdirs(SOURCE_PATH + "/7");
    mkdirs(SOURCE_PATH + "/7/8");
    touchFile(SOURCE_PATH + "/7/8/9.gz");
  }

  private static void mkdirs(String path) throws Exception {
    FileSystem fileSystem = cluster.getFileSystem();
    final Path qualifiedPath = new Path(path).makeQualified(fileSystem);
    pathList.add(qualifiedPath);
    fileSystem.mkdirs(qualifiedPath);
  }

  private static void touchFile(String path) throws Exception {
    FileSystem fs;
    DataOutputStream outputStream = null;
    GzipCodec gzipCodec = ReflectionUtils.newInstance(
        GzipCodec.class, getConfigurationForCluster());
    Compressor gzipCompressor = CodecPool.getCompressor(gzipCodec);
    OutputStream compressedOut =null;
    try {
      fs = cluster.getFileSystem();
      final Path qualifiedPath = new Path(path).makeQualified(fs);
      final long blockSize = fs.getDefaultBlockSize() * 2;
      outputStream = fs.create(qualifiedPath, true, 0,
              (short)(fs.getDefaultReplication()*2),
              blockSize);
      compressedOut = gzipCodec.createOutputStream(outputStream,
          gzipCompressor);
      compressedOut.write(new byte[FILE_SIZE]);
      compressedOut.write("\n".getBytes());
      compressedOut.flush();
      //outputStream.write(new byte[FILE_SIZE]);
      pathList.add(qualifiedPath);
    }
    finally {
      compressedOut.close();
      IOUtils.cleanup(null, outputStream);
      CodecPool.returnCompressor(gzipCompressor);
    }
  }

  private static void clearState() throws Exception {
    pathList.clear();
    cluster.getFileSystem().delete(new Path(TARGET_PATH));
    createSourceData();
  }

  @Test
  public void testUniformSizeDistCp() throws Exception {
    try {
      clearState();
      Path sourcePath = new Path(SOURCE_PATH)
              .makeQualified(cluster.getFileSystem());
      List<Path> sources = new ArrayList<Path>();
      sources.add(sourcePath);

      Path targetPath = new Path(TARGET_PATH)
              .makeQualified(cluster.getFileSystem());
      DistCpOptions options = new DistCpOptions(sources, targetPath);
      options.setOutPutDirectory(counterOutputPath);
      options.setAtomicCommit(true);
      options.setBlocking(false);
      Job job = new DistCp(configuration, options).execute();
      Path workDir = CopyOutputFormat.getWorkingDirectory(job);
      Path finalDir = CopyOutputFormat.getCommitDirectory(job);

      while (!job.isComplete()) {
        if (cluster.getFileSystem().exists(workDir)) {
          break;
        }
      }
      job.waitForCompletion(true);
      Assert.assertFalse(cluster.getFileSystem().exists(workDir));
      Assert.assertTrue(cluster.getFileSystem().exists(finalDir));
      Assert.assertFalse(cluster.getFileSystem().exists(
          new Path(job.getConfiguration().get(DistCpConstants.CONF_LABEL_META_FOLDER))));
      verifyResults();
    }
    catch (Exception e) {
      LOG.error("Exception encountered", e);
      Assert.fail("Unexpected exception: " + e.getMessage());
    }
  }

  @Test
  public void testCleanup() {
    try {
      clearState();
      Path sourcePath = new Path("noscheme:///file");
      List<Path> sources = new ArrayList<Path>();
      sources.add(sourcePath);

      Path targetPath = new Path(TARGET_PATH)
              .makeQualified(cluster.getFileSystem());
      DistCpOptions options = new DistCpOptions(sources, targetPath);
      options.setOutPutDirectory(counterOutputPath);

      Path stagingDir = JobSubmissionFiles.getStagingDir(
              new JobClient(new JobConf(configuration)), configuration);
      stagingDir.getFileSystem(configuration).mkdirs(stagingDir);

      try {
        new DistCp(configuration, options).execute();
      } catch (Throwable t) {
        Assert.assertEquals(stagingDir.getFileSystem(configuration).
            listStatus(stagingDir).length, 0);
      }
    } catch (Exception e) {
      LOG.error("Exception encountered ", e);
      Assert.fail("testCleanup failed " + e.getMessage());
    }
  }

  @Test
  public void testRootPath() throws Exception {
    try {
      clearState();
      List<Path> sources = new ArrayList<Path>();
      sources.add(new Path("/a")
              .makeQualified(cluster.getFileSystem()));
      sources.add(new Path("/b")
              .makeQualified(cluster.getFileSystem()));
      touchFile("/a/a.gz");
      touchFile("/b/b.gz");

      Path targetPath = new Path("/c")
              .makeQualified(cluster.getFileSystem());
      DistCpOptions options = new DistCpOptions(sources, targetPath);
      options.setOutPutDirectory(counterOutputPath);
      new DistCp(configuration, options).execute();
      Assert.assertTrue(cluster.getFileSystem().exists(new Path("/c/a/a.gz")));
      Assert.assertTrue(cluster.getFileSystem().exists(new Path("/c/b/b.gz")));
    }
    catch (Exception e) {
      LOG.error("Exception encountered", e);
      Assert.fail("Unexpected exception: " + e.getMessage());
    }
  }

  @Test
  public void testDynamicDistCp() throws Exception {
    try {
      clearState();
      Path sourcePath = new Path(SOURCE_PATH)
              .makeQualified(cluster.getFileSystem());
      List<Path> sources = new ArrayList<Path>();
      sources.add(sourcePath);

      Path targetPath = new Path(TARGET_PATH)
              .makeQualified(cluster.getFileSystem());
      DistCpOptions options = new DistCpOptions(sources, targetPath);
      options.setCopyStrategy("dynamic");
      options.setOutPutDirectory(counterOutputPath);

      options.setAtomicCommit(true);
      options.setAtomicWorkPath(new Path("/work"));
      options.setBlocking(false);
      Job job = new DistCp(configuration, options).execute();
      Path workDir = CopyOutputFormat.getWorkingDirectory(job);
      Path finalDir = CopyOutputFormat.getCommitDirectory(job);

      while (!job.isComplete()) {
        if (cluster.getFileSystem().exists(workDir)) {
          break;
        }
      }
      job.waitForCompletion(true);
      Assert.assertFalse(cluster.getFileSystem().exists(workDir));
      Assert.assertTrue(cluster.getFileSystem().exists(finalDir));

      verifyResults();
    }
    catch (Exception e) {
      LOG.error("Exception encountered", e);
      Assert.fail("Unexpected exception: " + e.getMessage());
    }
  }

  private static void verifyResults() throws Exception {
    for (Path path : pathList) {
      FileSystem fs = cluster.getFileSystem();

      Path sourcePath = path.makeQualified(fs);
      Path targetPath
              = new Path(sourcePath.toString().replaceAll(SOURCE_PATH, TARGET_PATH));

      Assert.assertTrue(fs.exists(targetPath));
      Assert.assertEquals(fs.isFile(sourcePath), fs.isFile(targetPath));
    }
  }
}
