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

import com.inmobi.conduit.ConduitConstants;
import com.inmobi.conduit.distcp.tools.DistCpConstants;
import com.inmobi.conduit.distcp.tools.DistCpOptionSwitch;
import com.inmobi.conduit.distcp.tools.DistCpOptions;
import com.inmobi.conduit.distcp.tools.mapred.CopyMapper;
import com.inmobi.conduit.distcp.tools.util.DistCpUtils;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.inmobi.messaging.Message;
import com.inmobi.messaging.util.AuditUtil;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;

public class TestCopyMapper {
  private static final Log LOG = LogFactory.getLog(TestCopyMapper.class);
  private static List<Path> pathList = new ArrayList<Path>();
  private static int nFiles = 0;
  private static final int FILE_SIZE = 1024;
  private static final long NUMBER_OF_MESSAGES_PER_FILE = 3;

  private static MiniDFSCluster cluster;

  private static final String SOURCE_PATH = "/tmp/source";
  private static final String TARGET_PATH = "/tmp/target";

  private static Configuration configuration;
  private static long currentTimestamp = new Date().getTime();
  private static long nextMinuteTimeStamp = currentTimestamp + 60000;

  @BeforeClass
  public static void setup() throws Exception {
    configuration = getConfigurationForCluster();
    cluster = new MiniDFSCluster(configuration, 1, true, null);
  }

  private static Configuration getConfigurationForCluster() throws IOException {
    Configuration configuration = new Configuration();
    System.setProperty("test.build.data", "target/tmp/build/TEST_COPY_MAPPER/data");
    configuration.set("hadoop.log.dir", "target/tmp");
    LOG.debug("fs.default.name  == " + configuration.get("fs.default.name"));
    LOG.debug("dfs.http.address == " + configuration.get("dfs.http.address"));
    return configuration;
  }

  private static Configuration getConfiguration() throws IOException {
    Configuration configuration = getConfigurationForCluster();
    Path workPath = new Path(TARGET_PATH)
            .makeQualified(cluster.getFileSystem());
    configuration.set(DistCpConstants.CONF_LABEL_TARGET_WORK_PATH,
            workPath.toString());
    configuration.set(DistCpConstants.CONF_LABEL_TARGET_FINAL_PATH,
            workPath.toString());
    configuration.setBoolean(DistCpOptionSwitch.OVERWRITE.getConfigLabel(),
            false);
    configuration.setBoolean(DistCpOptionSwitch.SKIP_CRC.getConfigLabel(),
            true);
    configuration.setBoolean(DistCpOptionSwitch.SYNC_FOLDERS.getConfigLabel(),
            true);
    configuration.set(DistCpOptionSwitch.PRESERVE_STATUS.getConfigLabel(),
            "br");
    return configuration;
  }

  private static void createSourceData() throws Exception {
    mkdirs(SOURCE_PATH + "/1");
    mkdirs(SOURCE_PATH + "/2");
    mkdirs(SOURCE_PATH + "/2/3/4");
    mkdirs(SOURCE_PATH + "/2/3");
    mkdirs(SOURCE_PATH + "/5");
    touchFile(SOURCE_PATH + "/streams/test1/6.gz");
    mkdirs(SOURCE_PATH + "/7");
    mkdirs(SOURCE_PATH + "/7/8");
    touchFile(SOURCE_PATH + "/7/streams/test1/9.gz");
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
        GzipCodec.class, getConfiguration());
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
      Message msg = new Message("generating test data".getBytes());
      AuditUtil.attachHeaders(msg, currentTimestamp);
      byte[] encodeMsg = Base64.encodeBase64(msg.getData().array());
      compressedOut.write(encodeMsg);
      compressedOut.write("\n".getBytes());
      compressedOut.write(encodeMsg);
      compressedOut.write("\n".getBytes());
      // Genearate a msg with different timestamp.  Default window period is 60sec
      AuditUtil.attachHeaders(msg, nextMinuteTimeStamp);
      encodeMsg = Base64.encodeBase64(msg.getData().array());
      compressedOut.write(encodeMsg);
      compressedOut.write("\n".getBytes());
      compressedOut.flush();
      compressedOut.close();
      pathList.add(qualifiedPath);
      ++nFiles;

      FileStatus fileStatus = fs.getFileStatus(qualifiedPath);
      System.out.println(fileStatus.getBlockSize());
      System.out.println(fileStatus.getReplication());
    }
    finally {
      compressedOut.close();
      IOUtils.cleanup(null, outputStream);
      CodecPool.returnCompressor(gzipCompressor);
    }
  }

  private static class StubStatusReporter extends StatusReporter {

    private Counters counters = new Counters();

    @Override
    public Counter getCounter(Enum<?> name) {
      return counters.findCounter(name);
    }

    @Override
    public Counter getCounter(String group, String name) {
      return counters.findCounter(group, name);
    }

    @Override
    public void progress() {}
    
    public float getProgress() { return (float) 1.0;}

    @Override
    public void setStatus(String status) {}
  }

  private static Mapper<Text, FileStatus, NullWritable, Text>.Context
  getMapperContext(CopyMapper copyMapper, final StatusReporter reporter,
                   final InMemoryWriter writer)
          throws IOException, InterruptedException {
    Mapper.Context ctx = Mockito.mock(Mapper.Context.class);
    Mockito.when(ctx.getConfiguration()).thenReturn(getConfiguration());
    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws
        Throwable {
        writer.write((NullWritable)invocationOnMock.getArguments()[0],
          (Text)invocationOnMock.getArguments()[1]);
        return null;
      }
    }).when(ctx).write(Mockito.any(), Mockito.any());

    Mockito.doAnswer(new Answer<Counter>() {
      @Override
      public Counter answer(InvocationOnMock invocationOnMock) throws Throwable {
        return reporter.getCounter((Enum<?>) invocationOnMock.getArguments()[0]);
      }
    }).when(ctx).getCounter(Mockito.any(CopyMapper.Counter.class));

    Mockito.doAnswer(new Answer<Counter>() {
      @Override
      public Counter answer(InvocationOnMock invocationOnMock) throws Throwable {
        return reporter.getCounter((String)invocationOnMock.getArguments()[0],
            (String)invocationOnMock.getArguments()[1]);
      }
    }).when(ctx).getCounter(Mockito.any(String.class), Mockito.any(String.class));

    final TaskAttemptID id = Mockito.mock(TaskAttemptID.class);
    Mockito.when(id.toString()).thenReturn("attempt1");
    Mockito.doAnswer(new Answer<TaskAttemptID>(){

      @Override
      public TaskAttemptID answer(InvocationOnMock invocationOnMock) throws Throwable {
        return id;
      }
    }).when(ctx).getTaskAttemptID();
    return ctx;
  }


  private static class InMemoryWriter extends RecordWriter<NullWritable, Text> {
    List<NullWritable> keys = new ArrayList<NullWritable>();
    List<Text> values = new ArrayList<Text>();

    @Override
    public void write(NullWritable key, Text value) throws IOException, InterruptedException {
      keys.add(key);
      values.add(value);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    }

    public List<NullWritable> keys() {
      return keys;
    }

    public List<Text> values() {
      return values;
    }
  }

  @Test
  public void testRun() {
    try {
      deleteState();
      createSourceData();

      FileSystem fs = cluster.getFileSystem();
      CopyMapper copyMapper = new CopyMapper();
      StatusReporter reporter = new StubStatusReporter();
      InMemoryWriter writer = new InMemoryWriter();
      Mapper<Text, FileStatus, NullWritable, Text>.Context context
              = getMapperContext(copyMapper, reporter, writer);
      copyMapper.setup(context);

      for (Path path: pathList) {
        copyMapper.map(new Text(DistCpUtils.getRelativePath(new Path(SOURCE_PATH), path)),
                fs.getFileStatus(path), context);
      }
      // Check that the maps worked.
      for (Path path : pathList) {
        final Path targetPath = new Path(path.toString()
                .replaceAll(SOURCE_PATH, TARGET_PATH));
        Assert.assertTrue(fs.exists(targetPath));
        Assert.assertTrue(fs.isFile(targetPath) == fs.isFile(path));
        Assert.assertEquals(fs.getFileStatus(path).getReplication(),
                fs.getFileStatus(targetPath).getReplication());
        Assert.assertEquals(fs.getFileStatus(path).getBlockSize(),
                fs.getFileStatus(targetPath).getBlockSize());
        Assert.assertTrue(!fs.isFile(targetPath) ||
                fs.getFileChecksum(targetPath).equals(
                        fs.getFileChecksum(path)));
      }

      Assert.assertEquals(pathList.size(),
              reporter.getCounter(CopyMapper.Counter.PATHS_COPIED).getValue());
      // Here file is compressed file. So, we should compare the file length
      // with the number of bytes read
      long totalSize = 0;
      for (Path path : pathList) {
        totalSize += fs.getFileStatus(path).getLen();
      }
      Assert.assertEquals(totalSize,
          reporter.getCounter(CopyMapper.Counter.BYTES_COPIED).getValue());
      long totalCounterValue = 0;
      for (Text value : writer.values()) {
        String tmp[] = value.toString().split(ConduitConstants.
            AUDIT_COUNTER_NAME_DELIMITER);
        Assert.assertEquals(4, tmp.length);
        Long numOfMsgs = Long.parseLong(tmp[3]);
        totalCounterValue += numOfMsgs;
      }
      Assert.assertEquals(nFiles * NUMBER_OF_MESSAGES_PER_FILE, totalCounterValue);
      testCopyingExistingFiles(fs, copyMapper, context);
    }
    catch (Exception e) {
      LOG.error("Unexpected exception: ", e);
      Assert.assertTrue(false);
    }
  }

  private void testCopyingExistingFiles(FileSystem fs, CopyMapper copyMapper,
                                        Mapper<Text, FileStatus, NullWritable, Text>.Context context) {

    try {
      for (Path path : pathList) {
        copyMapper.map(new Text(DistCpUtils.getRelativePath(new Path(SOURCE_PATH), path)),
                fs.getFileStatus(path), context);
      }

      Assert.assertEquals(nFiles,
              context.getCounter(CopyMapper.Counter.PATHS_SKIPPED).getValue());
    }
    catch (Exception exception) {
      Assert.assertTrue("Caught unexpected exception:" + exception.getMessage(),
              false);
    }
  }

  @Test
  public void testMakeDirFailure() {
    try {
      deleteState();
      createSourceData();

      FileSystem fs = cluster.getFileSystem();
      CopyMapper copyMapper = new CopyMapper();
      StatusReporter reporter = new StubStatusReporter();
      InMemoryWriter writer = new InMemoryWriter();
      Mapper<Text, FileStatus, NullWritable, Text>.Context context
              = getMapperContext(copyMapper, reporter, writer);

      Configuration configuration = context.getConfiguration();
      String workPath = new Path("hftp://localhost:1234/*/*/*/?/")
              .makeQualified(fs).toString();
      configuration.set(DistCpConstants.CONF_LABEL_TARGET_WORK_PATH,
              workPath);
      copyMapper.setup(context);

      copyMapper.map(new Text(DistCpUtils.getRelativePath(new Path(SOURCE_PATH), pathList.get(0))),
              fs.getFileStatus(pathList.get(0)), context);

      Assert.assertTrue("There should have been an exception.", false);
    }
    catch (Exception ignore) {
    }
  }

  @Test
  public void testIgnoreFailures() {
    doTestIgnoreFailures(true);
    doTestIgnoreFailures(false);
  }

  @Test
  public void testDirToFile() {
    try {
      deleteState();
      createSourceData();

      FileSystem fs = cluster.getFileSystem();
      CopyMapper copyMapper = new CopyMapper();
      StatusReporter reporter = new StubStatusReporter();
      InMemoryWriter writer = new InMemoryWriter();
      Mapper<Text, FileStatus, NullWritable, Text>.Context context
              = getMapperContext(copyMapper, reporter, writer);

      mkdirs(SOURCE_PATH + "/src/file");
      touchFile(TARGET_PATH + "/src/file");
      try {
        copyMapper.setup(context);
        copyMapper.map(new Text("/src/file"),
            fs.getFileStatus(new Path(SOURCE_PATH + "/src/file")),
            context);
      } catch (IOException e) {
        Assert.assertTrue(e.getMessage().startsWith("Can't replace"));
      }
    } catch (Exception e) {
      LOG.error("Exception encountered ", e);
      Assert.fail("Test failed: " + e.getMessage());
    }
  }

  @Test
  public void testPreserve() {
    try {
      deleteState();
      createSourceData();

      UserGroupInformation tmpUser = UserGroupInformation.createRemoteUser("guest");

      final CopyMapper copyMapper = new CopyMapper();
      
      final Mapper<Text, FileStatus, NullWritable, Text>.Context context =  tmpUser.
          doAs(new PrivilegedAction<Mapper<Text, FileStatus, NullWritable, Text>.Context>() {
        @Override
        public Mapper<Text, FileStatus, NullWritable, Text>.Context run() {
          try {
            StatusReporter reporter = new StubStatusReporter();
            InMemoryWriter writer = new InMemoryWriter();
            return getMapperContext(copyMapper, reporter, writer);
          } catch (Exception e) {
            LOG.error("Exception encountered ", e);
            throw new RuntimeException(e);
          }
        }
      });

      EnumSet<DistCpOptions.FileAttribute> preserveStatus =
          EnumSet.allOf(DistCpOptions.FileAttribute.class);

      context.getConfiguration().set(DistCpConstants.CONF_LABEL_PRESERVE_STATUS,
        DistCpUtils.packAttributes(preserveStatus));

      touchFile(SOURCE_PATH + "/src/file.gz");
      mkdirs(TARGET_PATH);
      cluster.getFileSystem().setPermission(new Path(TARGET_PATH), new FsPermission((short)511));

      final FileSystem tmpFS = tmpUser.doAs(new PrivilegedAction<FileSystem>() {
        @Override
        public FileSystem run() {
          try {
            return FileSystem.get(configuration);
          } catch (IOException e) {
            LOG.error("Exception encountered ", e);
            Assert.fail("Test failed: " + e.getMessage());
            throw new RuntimeException("Test ought to fail here");
          }
        }
      });

      tmpUser.doAs(new PrivilegedAction<Integer>() {
        @Override
        public Integer run() {
          try {
            copyMapper.setup(context);
            copyMapper.map(new Text("/src/file.gz"),
                tmpFS.getFileStatus(new Path(SOURCE_PATH + "/src/file.gz")),
                context);
            Assert.fail("Expected copy to fail");
          } catch (AccessControlException e) {
            Assert.assertTrue("Got exception: " + e.getMessage(), true);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          return null;
        }
      });
    } catch (Exception e) {
      LOG.error("Exception encountered ", e);
      Assert.fail("Test failed: " + e.getMessage());
    }
  }

  @Test
  public void testCopyReadableFiles() {
    try {
      deleteState();
      createSourceData();

      UserGroupInformation tmpUser = UserGroupInformation.createRemoteUser("guest");

      final CopyMapper copyMapper = new CopyMapper();

      final Mapper<Text, FileStatus, NullWritable, Text>.Context context =  tmpUser.
          doAs(new PrivilegedAction<Mapper<Text, FileStatus, NullWritable, Text>.Context>() {
        @Override
        public Mapper<Text, FileStatus, NullWritable, Text>.Context run() {
          try {
            StatusReporter reporter = new StubStatusReporter();
            InMemoryWriter writer = new InMemoryWriter();
            return getMapperContext(copyMapper, reporter, writer);
          } catch (Exception e) {
            LOG.error("Exception encountered ", e);
            throw new RuntimeException(e);
          }
        }
      });

      touchFile(SOURCE_PATH + "/src/file.gz");
      mkdirs(TARGET_PATH);
      cluster.getFileSystem().setPermission(new Path(SOURCE_PATH + "/src/file.gz"),
          new FsPermission(FsAction.READ, FsAction.READ, FsAction.READ));
      cluster.getFileSystem().setPermission(new Path(TARGET_PATH), new FsPermission((short)511));

      final FileSystem tmpFS = tmpUser.doAs(new PrivilegedAction<FileSystem>() {
        @Override
        public FileSystem run() {
          try {
            return FileSystem.get(configuration);
          } catch (IOException e) {
            LOG.error("Exception encountered ", e);
            Assert.fail("Test failed: " + e.getMessage());
            throw new RuntimeException("Test ought to fail here");
          }
        }
      });

      tmpUser.doAs(new PrivilegedAction<Integer>() {
        @Override
        public Integer run() {
          try {
            copyMapper.setup(context);
            copyMapper.map(new Text("/src/file.gz"),
                tmpFS.getFileStatus(new Path(SOURCE_PATH + "/src/file.gz")),
                context);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          return null;
        }
      });
    } catch (Exception e) {
      LOG.error("Exception encountered ", e);
      Assert.fail("Test failed: " + e.getMessage());
    }
  }

  @Test
  public void testSkipCopyNoPerms() {
    try {
      deleteState();
      createSourceData();

      final InMemoryWriter writer = new InMemoryWriter();
      UserGroupInformation tmpUser = UserGroupInformation.createRemoteUser("guest");

      final CopyMapper copyMapper = new CopyMapper();

      final Mapper<Text, FileStatus, NullWritable, Text>.Context context =  tmpUser.
          doAs(new PrivilegedAction<Mapper<Text, FileStatus, NullWritable, Text>.Context>() {
        @Override
        public Mapper<Text, FileStatus, NullWritable, Text>.Context run() {
          try {
            StatusReporter reporter = new StubStatusReporter();
            return getMapperContext(copyMapper, reporter, writer);
          } catch (Exception e) {
            LOG.error("Exception encountered ", e);
            throw new RuntimeException(e);
          }
        }
      });

      EnumSet<DistCpOptions.FileAttribute> preserveStatus =
          EnumSet.allOf(DistCpOptions.FileAttribute.class);

      context.getConfiguration().set(DistCpConstants.CONF_LABEL_PRESERVE_STATUS,
        DistCpUtils.packAttributes(preserveStatus));

      touchFile(SOURCE_PATH + "/src/file.gz");
      touchFile(TARGET_PATH + "/src/file.gz");
      cluster.getFileSystem().setPermission(new Path(SOURCE_PATH + "/src/file.gz"),
          new FsPermission(FsAction.READ, FsAction.READ, FsAction.READ));
      cluster.getFileSystem().setPermission(new Path(TARGET_PATH + "/src/file.gz"),
          new FsPermission(FsAction.READ, FsAction.READ, FsAction.READ));

      final FileSystem tmpFS = tmpUser.doAs(new PrivilegedAction<FileSystem>() {
        @Override
        public FileSystem run() {
          try {
            return FileSystem.get(configuration);
          } catch (IOException e) {
            LOG.error("Exception encountered ", e);
            Assert.fail("Test failed: " + e.getMessage());
            throw new RuntimeException("Test ought to fail here");
          }
        }
      });

      tmpUser.doAs(new PrivilegedAction<Integer>() {
        @Override
        public Integer run() {
          try {
            copyMapper.setup(context);
            copyMapper.map(new Text("/src/file.gz"),
                tmpFS.getFileStatus(new Path(SOURCE_PATH + "/src/file.gz")),
                context);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          return null;
        }
      });
    } catch (Exception e) {
      LOG.error("Exception encountered ", e);
      Assert.fail("Test failed: " + e.getMessage());
    }
  }

  @Test
  public void testFailCopyWithAccessControlException() {
    try {
      deleteState();
      createSourceData();

      final InMemoryWriter writer = new InMemoryWriter();
      UserGroupInformation tmpUser = UserGroupInformation.createRemoteUser("guest");

      final CopyMapper copyMapper = new CopyMapper();

      final Mapper<Text, FileStatus, NullWritable, Text>.Context context =  tmpUser.
          doAs(new PrivilegedAction<Mapper<Text, FileStatus, NullWritable, Text>.Context>() {
        @Override
        public Mapper<Text, FileStatus, NullWritable, Text>.Context run() {
          try {
            StatusReporter reporter = new StubStatusReporter();
            return getMapperContext(copyMapper, reporter, writer);
          } catch (Exception e) {
            LOG.error("Exception encountered ", e);
            throw new RuntimeException(e);
          }
        }
      });

      EnumSet<DistCpOptions.FileAttribute> preserveStatus =
          EnumSet.allOf(DistCpOptions.FileAttribute.class);

      context.getConfiguration().set(DistCpConstants.CONF_LABEL_PRESERVE_STATUS,
        DistCpUtils.packAttributes(preserveStatus));

      touchFile(SOURCE_PATH + "/src/file");
      OutputStream out = cluster.getFileSystem().create(new Path(TARGET_PATH + "/src/file"));
      out.write("hello world".getBytes());
      out.close();
      cluster.getFileSystem().setPermission(new Path(SOURCE_PATH + "/src/file"),
          new FsPermission(FsAction.READ, FsAction.READ, FsAction.READ));
      cluster.getFileSystem().setPermission(new Path(TARGET_PATH + "/src/file"),
          new FsPermission(FsAction.READ, FsAction.READ, FsAction.READ));

      final FileSystem tmpFS = tmpUser.doAs(new PrivilegedAction<FileSystem>() {
        @Override
        public FileSystem run() {
          try {
            return FileSystem.get(configuration);
          } catch (IOException e) {
            LOG.error("Exception encountered ", e);
            Assert.fail("Test failed: " + e.getMessage());
            throw new RuntimeException("Test ought to fail here");
          }
        }
      });

      tmpUser.doAs(new PrivilegedAction<Integer>() {
        @Override
        public Integer run() {
          try {
            copyMapper.setup(context);
            copyMapper.map(new Text("/src/file"),
                tmpFS.getFileStatus(new Path(SOURCE_PATH + "/src/file")),
                context);
            Assert.fail("Didn't expect the file to be copied");
          } catch (AccessControlException ignore) {
          } catch (Exception e) {
            if (e.getCause() == null || !(e.getCause() instanceof AccessControlException)) {
              throw new RuntimeException(e);
            }
          }
          return null;
        }
      });
    } catch (Exception e) {
      LOG.error("Exception encountered ", e);
      Assert.fail("Test failed: " + e.getMessage());
    }
  }

  @Test
  public void testFileToDir() {
    try {
      deleteState();
      createSourceData();

      FileSystem fs = cluster.getFileSystem();
      CopyMapper copyMapper = new CopyMapper();
      StatusReporter reporter = new StubStatusReporter();
      InMemoryWriter writer = new InMemoryWriter();
      Mapper<Text, FileStatus, NullWritable, Text>.Context context
              = getMapperContext(copyMapper, reporter, writer);

      touchFile(SOURCE_PATH + "/src/file");
      mkdirs(TARGET_PATH + "/src/file");
      try {
        copyMapper.setup(context);
        copyMapper.map(new Text("/src/file"),
            fs.getFileStatus(new Path(SOURCE_PATH + "/src/file")),
            context);
      } catch (IOException e) {
        Assert.assertTrue(e.getMessage().startsWith("Can't replace"));
      }
    } catch (Exception e) {
      LOG.error("Exception encountered ", e);
      Assert.fail("Test failed: " + e.getMessage());
    }
  }

  private void doTestIgnoreFailures(boolean ignoreFailures) {
    try {
      deleteState();
      createSourceData();

      FileSystem fs = cluster.getFileSystem();
      CopyMapper copyMapper = new CopyMapper();
      StatusReporter reporter = new StubStatusReporter();
      InMemoryWriter writer = new InMemoryWriter();
      Mapper<Text, FileStatus, NullWritable, Text>.Context context
              = getMapperContext(copyMapper, reporter, writer);

      Configuration configuration = context.getConfiguration();
      configuration.setBoolean(
              DistCpOptionSwitch.IGNORE_FAILURES.getConfigLabel(),ignoreFailures);
      configuration.setBoolean(DistCpOptionSwitch.OVERWRITE.getConfigLabel(),
              true);
      configuration.setBoolean(DistCpOptionSwitch.SKIP_CRC.getConfigLabel(),
              true);
      copyMapper.setup(context);

      for (Path path : pathList) {
        final FileStatus fileStatus = fs.getFileStatus(path);
        if (!fileStatus.isDir()) {
          fs.delete(path, true);
          copyMapper.map(new Text(DistCpUtils.getRelativePath(new Path(SOURCE_PATH), path)),
                  fileStatus, context);
        }
      }
      if (ignoreFailures) {
        for (Text value : writer.values()) {
          Assert.assertTrue(value.toString() + " is not skipped", value.toString().startsWith("FAIL:"));
        }
      }
      Assert.assertTrue("There should have been an exception.", ignoreFailures);
    }
    catch (Exception e) {
      Assert.assertTrue("Unexpected exception: " + e.getMessage(),
              !ignoreFailures);
      e.printStackTrace();
    }
  }

  private static void deleteState() throws IOException {
    pathList.clear();
    nFiles = 0;
    cluster.getFileSystem().delete(new Path(SOURCE_PATH), true);
    cluster.getFileSystem().delete(new Path(TARGET_PATH), true);
  }

  @Test
  public void testPreserveBlockSizeAndReplication() {
    testPreserveBlockSizeAndReplicationImpl(true);
    testPreserveBlockSizeAndReplicationImpl(false);
  }

  private void testPreserveBlockSizeAndReplicationImpl(boolean preserve){
    try {

      deleteState();
      createSourceData();

      FileSystem fs = cluster.getFileSystem();
      CopyMapper copyMapper = new CopyMapper();
      StatusReporter reporter = new StubStatusReporter();
      InMemoryWriter writer = new InMemoryWriter();
      Mapper<Text, FileStatus, NullWritable, Text>.Context context
              = getMapperContext(copyMapper, reporter, writer);

      Configuration configuration = context.getConfiguration();
      EnumSet<DistCpOptions.FileAttribute> fileAttributes
              = EnumSet.noneOf(DistCpOptions.FileAttribute.class);
      if (preserve) {
        fileAttributes.add(DistCpOptions.FileAttribute.BLOCKSIZE);
        fileAttributes.add(DistCpOptions.FileAttribute.REPLICATION);
      }
      configuration.set(DistCpOptionSwitch.PRESERVE_STATUS.getConfigLabel(),
              DistCpUtils.packAttributes(fileAttributes));

      copyMapper.setup(context);

      for (Path path : pathList) {
        final FileStatus fileStatus = fs.getFileStatus(path);
        copyMapper.map(new Text(DistCpUtils.getRelativePath(new Path(SOURCE_PATH), path)),
                fileStatus, context);
      }

      // Check that the block-size/replication aren't preserved.
      for (Path path : pathList) {
        final Path targetPath = new Path(path.toString()
                .replaceAll(SOURCE_PATH, TARGET_PATH));
        final FileStatus source = fs.getFileStatus(path);
        final FileStatus target = fs.getFileStatus(targetPath);
        if (!source.isDir() ) {
          Assert.assertTrue(preserve ||
                  source.getBlockSize() != target.getBlockSize());
          Assert.assertTrue(preserve ||
                  source.getReplication() != target.getReplication());
          Assert.assertTrue(!preserve ||
                  source.getBlockSize() == target.getBlockSize());
          Assert.assertTrue(!preserve ||
                  source.getReplication() == target.getReplication());
        }
      }
    }
    catch (Exception e) {
      Assert.assertTrue("Unexpected exception: " + e.getMessage(), false);
      e.printStackTrace();
    }
  }

  private static void changeUserGroup(String user, String group)
          throws IOException {
    FileSystem fs = cluster.getFileSystem();
    FsPermission changedPermission = new FsPermission(
            FsAction.ALL, FsAction.ALL, FsAction.ALL
    );
    for (Path path : pathList)
      if (fs.isFile(path)) {
        fs.setOwner(path, user, group);
        fs.setPermission(path, changedPermission);
      }
  }

  /**
   * If a single file is being copied to a location where the file (of the same
   * name) already exists, then the file shouldn't be skipped.
   */
  @Test
  public void testSingleFileCopy() {
    try {
      deleteState();
      touchFile(SOURCE_PATH + "/1.gz");
      Path sourceFilePath = pathList.get(0);
      Path targetFilePath = new Path(sourceFilePath.toString().replaceAll(
              SOURCE_PATH, TARGET_PATH));
      touchFile(targetFilePath.toString());

      FileSystem fs = cluster.getFileSystem();
      CopyMapper copyMapper = new CopyMapper();
      StatusReporter reporter = new StubStatusReporter();
      InMemoryWriter writer = new InMemoryWriter();
      Mapper<Text, FileStatus, NullWritable, Text>.Context context
              = getMapperContext(copyMapper, reporter, writer);

      context.getConfiguration().set(
              DistCpConstants.CONF_LABEL_TARGET_FINAL_PATH,
              targetFilePath.getParent().toString()); // Parent directory.
      copyMapper.setup(context);

      final FileStatus sourceFileStatus = fs.getFileStatus(sourceFilePath);

      long before = fs.getFileStatus(targetFilePath).getModificationTime();
      copyMapper.map(new Text(DistCpUtils.getRelativePath(
              new Path(SOURCE_PATH), sourceFilePath)), sourceFileStatus, context);
      long after = fs.getFileStatus(targetFilePath).getModificationTime();

      Assert.assertTrue("File should have been skipped", before == after);

      context.getConfiguration().set(
              DistCpConstants.CONF_LABEL_TARGET_FINAL_PATH,
              targetFilePath.toString()); // Specify the file path.
      copyMapper.setup(context);

      before = fs.getFileStatus(targetFilePath).getModificationTime();
      try { Thread.sleep(2); } catch (Throwable ignore) {}
      copyMapper.map(new Text(DistCpUtils.getRelativePath(
              new Path(SOURCE_PATH), sourceFilePath)), sourceFileStatus, context);
      after = fs.getFileStatus(targetFilePath).getModificationTime();

      Assert.assertTrue("File should have been overwritten.", before < after);

    } catch (Exception exception) {
      Assert.fail("Unexpected exception: " + exception.getMessage());
      exception.printStackTrace();
    }
  }

  @Test
  public void testPreserveUserGroup() {
    testPreserveUserGroupImpl(true);
    testPreserveUserGroupImpl(false);
  }

  private void testPreserveUserGroupImpl(boolean preserve){
    try {

      deleteState();
      createSourceData();
      changeUserGroup("Michael", "Corleone");

      FileSystem fs = cluster.getFileSystem();
      CopyMapper copyMapper = new CopyMapper();
      StatusReporter reporter = new StubStatusReporter();
      InMemoryWriter writer = new InMemoryWriter();
      Mapper<Text, FileStatus, NullWritable, Text>.Context context
              = getMapperContext(copyMapper, reporter, writer);

      Configuration configuration = context.getConfiguration();
      EnumSet<DistCpOptions.FileAttribute> fileAttributes
              = EnumSet.noneOf(DistCpOptions.FileAttribute.class);
      if (preserve) {
        fileAttributes.add(DistCpOptions.FileAttribute.USER);
        fileAttributes.add(DistCpOptions.FileAttribute.GROUP);
        fileAttributes.add(DistCpOptions.FileAttribute.PERMISSION);
      }

      configuration.set(DistCpOptionSwitch.PRESERVE_STATUS.getConfigLabel(),
              DistCpUtils.packAttributes(fileAttributes));
      copyMapper.setup(context);

      for (Path path : pathList) {
        final FileStatus fileStatus = fs.getFileStatus(path);
        copyMapper.map(new Text(DistCpUtils.getRelativePath(new Path(SOURCE_PATH), path)),
                fileStatus, context);
      }

      // Check that the user/group attributes are preserved
      // (only) as necessary.
      for (Path path : pathList) {
        final Path targetPath = new Path(path.toString()
                .replaceAll(SOURCE_PATH, TARGET_PATH));
        final FileStatus source = fs.getFileStatus(path);
        final FileStatus target = fs.getFileStatus(targetPath);
        if (!source.isDir()) {
          Assert.assertTrue(!preserve || source.getOwner().equals(target.getOwner()));
          Assert.assertTrue(!preserve || source.getGroup().equals(target.getGroup()));
          Assert.assertTrue(!preserve || source.getPermission().equals(target.getPermission()));
          Assert.assertTrue( preserve || !source.getOwner().equals(target.getOwner()));
          Assert.assertTrue( preserve || !source.getGroup().equals(target.getGroup()));
          Assert.assertTrue( preserve || !source.getPermission().equals(target.getPermission()));
          Assert.assertTrue(source.isDir() ||
                  source.getReplication() != target.getReplication());
        }
      }
    }
    catch (Exception e) {
      Assert.assertTrue("Unexpected exception: " + e.getMessage(), false);
      e.printStackTrace();
    }
  }
}
