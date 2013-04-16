package com.inmobi.databus.local;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfigParser;
import com.inmobi.databus.FSCheckpointProvider;

public class TestLocalStreamCommit {

  static Path rootDir = new Path("/tmp/test-databus/databus/");

  static FileSystem localFs;
  private List<String> streamsToProcess = new ArrayList<String>();
   
  private void createData(Cluster cluster) throws IOException {
    Path tmpPath = new Path(cluster.getTmpPath(),
        LocalStreamService.class.getName());
    Path tmpJobOutputPath = new Path(tmpPath, "jobOut");
    Path path1 = new Path(tmpJobOutputPath, "stream1");
    Path path2 = new Path(tmpJobOutputPath, "stream2");
    streamsToProcess.add("stream1");
    streamsToProcess.add("stream2");
    localFs.mkdirs(path1);
    localFs.mkdirs(path2);
    localFs.create(new Path(path1, "file1"));
    localFs.create(new Path(path2, "file2"));
  }

  @BeforeTest
  public void setUP() throws Exception {
    localFs = FileSystem.getLocal(new Configuration());
    //clean up the test data if any thing is left in the previous runs
    cleanup();
  }

  @AfterTest
  public void cleanup() throws Exception {
    localFs.delete(rootDir, true);

  }

  @Test
  public void testPrepareForCommit() throws Exception {
    DatabusConfigParser parser = new DatabusConfigParser(
        "src/test/resources/test-merge-mirror-databus1.xml");

    Cluster cluster1 = parser.getConfig().getClusters().get("testcluster1");
    LocalStreamService service = new LocalStreamService(parser.getConfig(),
        cluster1, null, new FSCheckpointProvider(
            "/tmp/test-databus/databus/checkpoint"), streamsToProcess);
    createData(cluster1);
    service.prepareForCommit(System.currentTimeMillis());
    Path tmpPath = new Path(cluster1.getTmpPath(),
        LocalStreamService.class.getName());
    Path tmpConsumerPath = new Path(tmpPath, "testcluster2");
    FileStatus[] status = localFs.listStatus(tmpConsumerPath);
    for (FileStatus tmpStatus : status) {
      // opening the consumer file written for testcluster2
      // it should not have any entry for stream 1 as testcluster2 is primary
      // destination only for stream2
      FSDataInputStream inStream = localFs.open(tmpStatus.getPath());
      String line;
      while ((line = inStream.readLine()) != null) {
        assert (!line.contains("stream1"));
      }

    }
  }
}
