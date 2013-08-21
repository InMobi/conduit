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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.tools.CopyListing;
import org.apache.hadoop.tools.DistCpConstants;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.tools.DistCpOptions.FileAttribute;
import org.apache.hadoop.tools.GlobbedCopyListing;
import org.apache.hadoop.tools.util.TestDistCpUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.mapred.MockJobTracker;
import org.junit.*;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.Stack;

public class TestCopyCommitter {
  private static final Log LOG = LogFactory.getLog(TestCopyCommitter.class);

  private static final Random rand = new Random();

  private static final Counters EMPTY_COUNTERS = new Counters();
  private static final Credentials CREDENTIALS = new Credentials();

  private static Configuration config;
  private static MiniDFSCluster cluster;
  private static CounterProvider counterProvider;

  @BeforeClass
  public static void create() throws IOException {
    config = MockJobTracker.getJobForClient().getConfiguration();
    counterProvider = new CounterProvider(EMPTY_COUNTERS);
    config.setLong(DistCpConstants.CONF_LABEL_TOTAL_BYTES_TO_BE_COPIED, 0);
    cluster = new MiniDFSCluster(config, 1, true, null);
  }

  @AfterClass
  public static void destroy() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Before
  public void createMetaFolder() {
    config.set(DistCpConstants.CONF_LABEL_META_FOLDER, "/meta");
    Path meta = new Path("/meta");
    try {
      cluster.getFileSystem().mkdirs(meta);
    } catch (IOException e) {
      LOG.error("Exception encountered while creating meta folder", e);
      Assert.fail("Unable to create meta folder");
    }
  }

  @After
  public void cleanupMetaFolder() {
    Path meta = new Path("/meta");
    try {
      if (cluster.getFileSystem().exists(meta)) {
        cluster.getFileSystem().delete(meta, true);
        Assert.fail("Expected meta folder to be deleted");
      }
    } catch (IOException e) {
      LOG.error("Exception encountered while cleaning up folder", e);
      Assert.fail("Unable to clean up meta folder");
    }
  }

  @Test
  public void testNoCommitAction() {

    TaskAttemptContext taskAttemptContext = getTaskAttemptContext(config);
    JobContext jobContext = Mockito.mock(JobContext.class);
    Mockito.when(jobContext.getConfiguration()).thenReturn(config);
    JobID jobID = new JobID();
    Mockito.when(jobContext.getJobID()).thenReturn(jobID);
    final String[] statusString = new String[1];
    try {
      Mockito.doAnswer(new Answer() {
        @Override
        public Object answer(InvocationOnMock invocationOnMock) throws
          Throwable {
          statusString[0] = (String) invocationOnMock.getArguments()[0];
          return null;  //To change body of implemented methods use File | Settings | File Templates.
        }
      }).when(taskAttemptContext).setStatus(Mockito.anyString());
    } catch (Throwable e) {
    }
    try {
      OutputCommitter committer = new CopyCommitter(null, taskAttemptContext);
      committer.commitJob(jobContext);
      Assert.assertEquals(statusString[0], "Commit Successful");

      //Test for idempotent commit
      committer.commitJob(jobContext);
      Assert.assertEquals(statusString[0], "Commit Successful");
    } catch (IOException e) {
      LOG.error("Exception encountered ", e);
      Assert.fail("Commit failed");
    }
  }

  @Test
  public void testValidationPass() {

    config.setLong(DistCpConstants.CONF_LABEL_TOTAL_BYTES_TO_BE_COPIED, 100);
    Counters counters = new Counters();
    CounterGroup grp = counters.getGroup(CopyMapper.Counter.class.getName());
    grp.findCounter(CopyMapper.Counter.BYTES_COPIED.name()).increment(50);
    grp.findCounter(CopyMapper.Counter.BYTES_FAILED.name()).increment(20);
    grp.findCounter(CopyMapper.Counter.BYTES_SKIPPED.name()).increment(30);
    counterProvider.setCounters(counters);

    try {
      TaskAttemptContext taskAttemptContext = getTaskAttemptContext(config);
      JobContext jobContext = Mockito.mock(JobContext.class);
      Mockito.when(jobContext.getConfiguration()).thenReturn(config);
      JobID jobID = new JobID();
      Mockito.when(jobContext.getJobID()).thenReturn(jobID);
      final String[] statusString = new String[1];
      try {
        Mockito.doAnswer(new Answer() {
          @Override
          public Object answer(InvocationOnMock invocationOnMock) throws
            Throwable {
            LOG.info("XXXX  crap I am called now " + invocationOnMock.getArguments()[0]);
            statusString[0] = (String) invocationOnMock.getArguments()[0];
            return null;  //To change body of implemented methods use File | Settings | File Templates.
          }
        }).when(taskAttemptContext).setStatus(Mockito.anyString());
      } catch (Throwable e) {
      }
      try {
        OutputCommitter committer = new CopyCommitter(null, taskAttemptContext);
        committer.commitJob(jobContext);
        Assert.assertEquals(statusString[0], "Commit Successful");
      } catch (IOException e) {
        LOG.error("Exception encountered ", e);
        Assert.fail("Commit failed");
      }
    } finally {
      config.setLong(DistCpConstants.CONF_LABEL_TOTAL_BYTES_TO_BE_COPIED, 0);
      counterProvider.setCounters(EMPTY_COUNTERS);
    }
  }

  @Test
  public void testPreserveStatus() {
    TaskAttemptContext taskAttemptContext = getTaskAttemptContext(config);

    JobContext jobContext = Mockito.mock(JobContext.class);
    Mockito.when(jobContext.getConfiguration()).thenReturn(config);
    JobID jobID = new JobID();
    Mockito.when(jobContext.getJobID()).thenReturn(jobID);
    Configuration conf = jobContext.getConfiguration();


    String sourceBase;
    String targetBase;
    FileSystem fs = null;
    try {
      OutputCommitter committer = new CopyCommitter(null, taskAttemptContext);
      fs = FileSystem.get(conf);
      FsPermission sourcePerm = new FsPermission((short) 511);
      FsPermission initialPerm = new FsPermission((short) 448);
      sourceBase = TestDistCpUtils.createTestSetup(fs, sourcePerm);
      targetBase = TestDistCpUtils.createTestSetup(fs, initialPerm);

      DistCpOptions options = new DistCpOptions(Arrays.asList(new Path(sourceBase)),
        new Path("/out"));
      options.preserve(FileAttribute.PERMISSION);
      options.appendToConf(conf);

      CopyListing listing = new GlobbedCopyListing(conf, CREDENTIALS);
      Path listingFile = new Path("/tmp1/" + String.valueOf(rand.nextLong()));
      listing.buildListing(listingFile, options);

      conf.set(DistCpConstants.CONF_LABEL_TARGET_WORK_PATH, targetBase);

      committer.commitJob(jobContext);
      if (!checkDirectoryPermissions(fs, targetBase, sourcePerm)) {
        Assert.fail("Permission don't match");
      }

      //Test for idempotent commit
      committer.commitJob(jobContext);
      if (!checkDirectoryPermissions(fs, targetBase, sourcePerm)) {
        Assert.fail("Permission don't match");
      }

    } catch (IOException e) {
      LOG.error("Exception encountered while testing for preserve status", e);
      Assert.fail("Preserve status failure");
    } finally {
      TestDistCpUtils.delete(fs, "/tmp1");
    }

  }

  @Test
  public void testDeleteMissing() {
    TaskAttemptContext taskAttemptContext = getTaskAttemptContext(config);
    JobContext jobContext = Mockito.mock(JobContext.class);
    Mockito.when(jobContext.getConfiguration()).thenReturn(config);
    JobID jobID = new JobID();
    Mockito.when(jobContext.getJobID()).thenReturn(jobID);
    Configuration conf = jobContext.getConfiguration();


    String sourceBase;
    String targetBase;
    FileSystem fs = null;
    try {
      OutputCommitter committer = new CopyCommitter(null, taskAttemptContext);
      fs = FileSystem.get(conf);
      sourceBase = TestDistCpUtils.createTestSetup(fs, FsPermission.getDefault());
      targetBase = TestDistCpUtils.createTestSetup(fs, FsPermission.getDefault());
      String targetBaseAdd = TestDistCpUtils.createTestSetup(fs, FsPermission.getDefault());
      fs.rename(new Path(targetBaseAdd), new Path(targetBase));

      DistCpOptions options = new DistCpOptions(Arrays.asList(new Path(sourceBase)),
        new Path("/out"));
      options.setSyncFolder(true);
      options.setDeleteMissing(true);
      options.appendToConf(conf);

      CopyListing listing = new GlobbedCopyListing(conf, CREDENTIALS);
      Path listingFile = new Path("/tmp1/" + String.valueOf(rand.nextLong()));
      listing.buildListing(listingFile, options);

      conf.set(DistCpConstants.CONF_LABEL_TARGET_WORK_PATH, targetBase);
      conf.set(DistCpConstants.CONF_LABEL_TARGET_FINAL_PATH, targetBase);

      committer.commitJob(jobContext);
      if (!TestDistCpUtils.checkIfFoldersAreInSync(fs, targetBase, sourceBase)) {
        Assert.fail("Source and target folders are not in sync");
      }
      if (!TestDistCpUtils.checkIfFoldersAreInSync(fs, sourceBase, targetBase)) {
        Assert.fail("Source and target folders are not in sync");
      }

      //Test for idempotent commit
      committer.commitJob(jobContext);
      if (!TestDistCpUtils.checkIfFoldersAreInSync(fs, targetBase, sourceBase)) {
        Assert.fail("Source and target folders are not in sync");
      }
      if (!TestDistCpUtils.checkIfFoldersAreInSync(fs, sourceBase, targetBase)) {
        Assert.fail("Source and target folders are not in sync");
      }
    } catch (Throwable e) {
      LOG.error("Exception encountered while testing for delete missing", e);
      Assert.fail("Delete missing failure");
    } finally {
      TestDistCpUtils.delete(fs, "/tmp1");
    }

  }

  @Test
  public void testDeleteMissingFlatInterleavedFiles() {
    TaskAttemptContext taskAttemptContext = getTaskAttemptContext(config);
    JobContext jobContext = Mockito.mock(JobContext.class);
    Mockito.when(jobContext.getConfiguration()).thenReturn(config);
    JobID jobID = new JobID();
    Mockito.when(jobContext.getJobID()).thenReturn(jobID);
    Configuration conf = jobContext.getConfiguration();


    String sourceBase;
    String targetBase;
    FileSystem fs = null;
    try {
      OutputCommitter committer = new CopyCommitter(null, taskAttemptContext);
      fs = FileSystem.get(conf);
      sourceBase = "/tmp1/" + String.valueOf(rand.nextLong());
      targetBase = "/tmp1/" + String.valueOf(rand.nextLong());
      TestDistCpUtils.createFile(fs, sourceBase + "/1");
      TestDistCpUtils.createFile(fs, sourceBase + "/3");
      TestDistCpUtils.createFile(fs, sourceBase + "/4");
      TestDistCpUtils.createFile(fs, sourceBase + "/5");
      TestDistCpUtils.createFile(fs, sourceBase + "/7");
      TestDistCpUtils.createFile(fs, sourceBase + "/8");
      TestDistCpUtils.createFile(fs, sourceBase + "/9");

      TestDistCpUtils.createFile(fs, targetBase + "/2");
      TestDistCpUtils.createFile(fs, targetBase + "/4");
      TestDistCpUtils.createFile(fs, targetBase + "/5");
      TestDistCpUtils.createFile(fs, targetBase + "/7");
      TestDistCpUtils.createFile(fs, targetBase + "/9");
      TestDistCpUtils.createFile(fs, targetBase + "/A");

      DistCpOptions options = new DistCpOptions(Arrays.asList(new Path(sourceBase)),
        new Path("/out"));
      options.setSyncFolder(true);
      options.setDeleteMissing(true);
      options.appendToConf(conf);

      CopyListing listing = new GlobbedCopyListing(conf, CREDENTIALS);
      Path listingFile = new Path("/tmp1/" + String.valueOf(rand.nextLong()));
      listing.buildListing(listingFile, options);

      conf.set(DistCpConstants.CONF_LABEL_TARGET_WORK_PATH, targetBase);
      conf.set(DistCpConstants.CONF_LABEL_TARGET_FINAL_PATH, targetBase);

      committer.commitJob(jobContext);
      if (!TestDistCpUtils.checkIfFoldersAreInSync(fs, targetBase, sourceBase)) {
        Assert.fail("Source and target folders are not in sync");
      }
      Assert.assertEquals(fs.listStatus(new Path(targetBase)).length, 4);

      //Test for idempotent commit
      committer.commitJob(jobContext);
      if (!TestDistCpUtils.checkIfFoldersAreInSync(fs, targetBase, sourceBase)) {
        Assert.fail("Source and target folders are not in sync");
      }
      Assert.assertEquals(fs.listStatus(new Path(targetBase)).length, 4);
    } catch (IOException e) {
      LOG.error("Exception encountered while testing for delete missing", e);
      Assert.fail("Delete missing failure");
    } finally {
      TestDistCpUtils.delete(fs, "/tmp1");
    }

  }

  @Test
  public void testAtomicCommitMissingFinal() {
    TaskAttemptContext taskAttemptContext = getTaskAttemptContext(config);
    JobContext jobContext = Mockito.mock(JobContext.class);
    Mockito.when(jobContext.getConfiguration()).thenReturn(config);
    JobID jobID = new JobID();
    Mockito.when(jobContext.getJobID()).thenReturn(jobID);
    Configuration conf = jobContext.getConfiguration();


    String workPath = "/tmp1/" + String.valueOf(rand.nextLong());
    String finalPath = "/tmp1/" + String.valueOf(rand.nextLong());
    FileSystem fs = null;
    try {
      OutputCommitter committer = new CopyCommitter(null, taskAttemptContext);
      fs = FileSystem.get(conf);
      fs.mkdirs(new Path(workPath));

      conf.set(DistCpConstants.CONF_LABEL_TARGET_WORK_PATH, workPath);
      conf.set(DistCpConstants.CONF_LABEL_TARGET_FINAL_PATH, finalPath);
      conf.setBoolean(DistCpConstants.CONF_LABEL_ATOMIC_COPY, true);
      //XXX set label to false explicitly, conf is not mixed up
      conf.setBoolean(DistCpConstants.CONF_LABEL_DELETE_MISSING, false);

      Assert.assertTrue(fs.exists(new Path(workPath)));
      Assert.assertFalse(fs.exists(new Path(finalPath)));
      committer.commitJob(jobContext);
      Assert.assertFalse(fs.exists(new Path(workPath)));
      Assert.assertTrue(fs.exists(new Path(finalPath)));

      //Test for idempotent commit
      committer.commitJob(jobContext);
      Assert.assertFalse(fs.exists(new Path(workPath)));
      Assert.assertTrue(fs.exists(new Path(finalPath)));

    } catch (IOException e) {
      LOG.error("Exception encountered while testing for preserve status", e);
      Assert.fail("Atomic commit failure");
    } finally {
      TestDistCpUtils.delete(fs, workPath);
      TestDistCpUtils.delete(fs, finalPath);
    }
  }

  @Test
  public void testAtomicCommitExistingFinal() {
    TaskAttemptContext taskAttemptContext = getTaskAttemptContext(config);
    JobContext jobContext = Mockito.mock(JobContext.class, Mockito.RETURNS_DEEP_STUBS);
    Mockito.when(jobContext.getConfiguration()).thenReturn(config);
    JobID jobID = new JobID();
    Mockito.when(jobContext.getJobID()).thenReturn(jobID);
    Configuration conf = jobContext.getConfiguration();


    String workPath = "/tmp1/" + String.valueOf(rand.nextLong());
    String finalPath = "/tmp1/" + String.valueOf(rand.nextLong());
    FileSystem fs = null;
    try {
      OutputCommitter committer = new CopyCommitter(null, taskAttemptContext);
      fs = FileSystem.get(conf);
      fs.mkdirs(new Path(workPath));
      fs.mkdirs(new Path(finalPath));

      conf.set(DistCpConstants.CONF_LABEL_TARGET_WORK_PATH, workPath);
      conf.set(DistCpConstants.CONF_LABEL_TARGET_FINAL_PATH, finalPath);
      conf.setBoolean(DistCpConstants.CONF_LABEL_ATOMIC_COPY, true);
      //XXX set label to false explicitly, conf is not mixed up
      conf.setBoolean(DistCpConstants.CONF_LABEL_DELETE_MISSING, false);

      Assert.assertTrue(fs.exists(new Path(workPath)));
      Assert.assertTrue(fs.exists(new Path(finalPath)));
      committer.commitJob(jobContext);
      Assert.assertFalse(fs.exists(new Path(workPath)));
      Assert.assertTrue(fs.exists(new Path(finalPath)));

      //Test for idempotent commit
      committer.commitJob(jobContext);
      Assert.assertFalse(fs.exists(new Path(workPath)));
      Assert.assertTrue(fs.exists(new Path(finalPath)));

    } catch (IOException e) {
      LOG.error("Exception encountered while testing for preserve status", e);
      Assert.fail("Atomic commit failure");
    } finally {
      TestDistCpUtils.delete(fs, workPath);
      TestDistCpUtils.delete(fs, finalPath);
    }
  }

  private TaskAttemptContext getTaskAttemptContext(Configuration conf) {
    TaskAttemptContext context = Mockito.mock(TaskAttemptContext.class);
    Mockito.when(context.getConfiguration()).thenReturn(conf);
    TaskAttemptID taskId = new TaskAttemptID("200707121733", 1, false, 1, 1);
    Mockito.when(context.getTaskAttemptID()).thenReturn(taskId);
    return context;
  }

  private boolean checkDirectoryPermissions(FileSystem fs, String targetBase,
                                            FsPermission sourcePerm) throws IOException {
    Path base = new Path(targetBase);

    Stack<Path> stack = new Stack<Path>();
    stack.push(base);
    while (!stack.isEmpty()) {
      Path file = stack.pop();
      if (!fs.exists(file)) continue;
      FileStatus[] fStatus = fs.listStatus(file);
      if (fStatus == null || fStatus.length == 0) continue;

      for (FileStatus status : fStatus) {
        if (status.isDir()) {
          stack.push(status.getPath());
          Assert.assertEquals(status.getPermission(), sourcePerm);
        }
      }
    }
    return true;
  }

  @Ignore
  @Test
  public void testCounterProvider() {
    try {
      Job job = MockJobTracker.getJobForClient();
      Counters a = EMPTY_COUNTERS;
      CounterGroup grp = a.getGroup("abc");
      Counter cntr = grp.findCounter("counter");
      cntr.increment(100);
      CounterProvider cp = new CounterProvider(a);
      job.submit();
      Assert.assertEquals(job.getCounters(), a);
    } catch (Exception e) {
      LOG.error("Exception encountered ", e);
    }
  }
}
