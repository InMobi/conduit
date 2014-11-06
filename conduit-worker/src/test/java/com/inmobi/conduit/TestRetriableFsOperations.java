package com.inmobi.conduit;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Date;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

public class TestRetriableFsOperations extends AbstractService {

  private static final Log LOG = LogFactory.getLog(
      TestRetriableFsOperations.class.getSimpleName());

  private int retryNumber = 0;
  private int exceptionCount = 0;
  private static final int NUM_RETRIES = 10;
  private Path rootDir;
  private FileSystem fs;

  static {
    System.setProperty(ConduitConstants.NUM_RETRIES,
        String.valueOf(NUM_RETRIES));
  }

  public TestRetriableFsOperations() {
    super(null, null, -1, null);
  }

  public TestRetriableFsOperations(String name, ConduitConfig config,
      long runIntervalInMsec, CheckpointProvider provider,
      Set<String> streamsToProcess) {
    super(name, config, runIntervalInMsec, provider, streamsToProcess);
  }

  @BeforeMethod
  public void setup() throws Exception {
    rootDir = new Path("/tmp",
        new Path(TestRetriableFsOperations.class.getSimpleName(), "rootdir"));
    fs = rootDir.getFileSystem(new Configuration());
    exceptionCount = 0;
    retryNumber = 0;
  }

  @AfterMethod
  public void cleanup() throws Exception {
    fs.delete(rootDir, true);
  }

  @Test
  public void testFsRename() throws Exception {
    final Path srcFile = new Path(rootDir, "srcFile");
    fs.mkdirs(rootDir);
    LOG.info("Creating  source file " + srcFile);
    fs.create(srcFile);
    final Path destFile = new Path(rootDir, "destFile");
    FileSystem mockFs = getMockFileSystem();

    Mockito.doAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) throws IOException {
        return doFsOperation(srcFile, destFile);
      }
    }).when(mockFs).rename(srcFile, destFile);

    try {
      boolean flag = retriableRename(mockFs, srcFile, destFile, null);
      Assert.assertTrue(flag);
      Assert.assertTrue(fs.exists(destFile) && !fs.exists(srcFile));
    } catch (Exception e) {
      Assert.assertTrue(e instanceof IOException);
    }
    Assert.assertEquals(exceptionCount, NUM_RETRIES/2);
  }

  private boolean doFsOperation(Path src, Path dst) throws IOException {
    if (retryNumber++ < NUM_RETRIES/2) {
      exceptionCount++;
      throw new IOException();
    } else {
      fs.rename(src, dst);
      return true;
    }
  }

  @Test
  public void testFsMkdirs() throws Exception {
    final Path dir = new Path(rootDir, "sample");
    FileSystem mockFs = getMockFileSystem();

    Mockito.doAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) throws IOException {
        if (retryNumber++ < NUM_RETRIES/2) {
          exceptionCount++;
          throw new IOException();
        } else {
          return fs.mkdirs(dir);
        }
      }
    }).when(mockFs).mkdirs(dir);

    try {
      boolean flag = retriableMkDirs(mockFs, dir, null);
      // flag should be true as dir will be created after NUM_RETRIES/2 attempts
      Assert.assertTrue(flag);
      Assert.assertTrue(fs.exists(dir));
    } catch (Exception e) {
      Assert.assertTrue(e instanceof IOException);
    }
    Assert.assertEquals(exceptionCount, NUM_RETRIES/2);
  }

  private FileSystem getMockFileSystem() {
    return Mockito.mock(FileSystem.class);
  }

  @Override
  public long getMSecondsTillNextRun(long currentTime) {
    return 0;
  }

  @Override
  protected void execute() throws Exception {

  }

  @Override
  public String getServiceType() {
    return null;
  }

  @Override
  protected String getTopicNameFromDestnPath(Path destnPath) {
    return null;
  }

  @Override
  protected String getTier() {
    return null;
  }

  @Override
  protected String getTableName(String stream) {
    return null;
  }
}
