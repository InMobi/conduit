package com.inmobi.databus.distcp;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.DestinationStream;
import com.inmobi.databus.FSCheckpointProvider;


public class TestDistCPBaseService  {
  private static Logger LOG = Logger.getLogger(TestDistCPBaseService.class);
  Path testRoot = new Path("/tmp/", this.getClass().getName());
  Path testRoot1=new Path("/tmp/","sample-test");
  FileSystem localFs;
  Cluster cluster;
  MirrorStreamService mirrorService = null;
  MergedStreamService mergeService = null;
  MergedStreamService mergedService1 = null;
  FileSystem srcFs = null;
  String expectedFileName1 = "/tmp/com.inmobi.databus.distcp"
      + ".TestDistCPBaseService/data-file1";
  String expectedFileName2 = "/tmp/com.inmobi.databus.distcp"
      + ".TestDistCPBaseService/data-file2";
  String expectedFileName3 = "/tmp/sample-test/data-file1";
  Set<String> expectedConsumePaths = new HashSet<String>();

  @BeforeTest
  public void setUP() throws Exception {
    //create fs
    localFs = FileSystem.getLocal(new Configuration());
    // clean up the test data if any thing is left in the previous runs
    cleanUP();
    localFs.mkdirs(testRoot);

    //create cluster
    Map<String, String> clusterConf = new HashMap<String, String>();
    clusterConf.put("hdfsurl", localFs.getUri().toString());
    clusterConf.put("jturl", "local");
    clusterConf.put("name", "databusCluster");
    clusterConf.put("jobqueuename", "default");
    Set<String> sourceNames = new HashSet<String>();
    sourceNames.add("stream1");
    Cluster cluster = new Cluster(clusterConf, testRoot.toString(),
        new HashMap<String, DestinationStream>(),
        sourceNames);

    // create mirror service
    mirrorService = new MirrorStreamService(null, cluster, cluster, null,
        new FSCheckpointProvider(cluster.getCheckpointDir()),
        new HashSet<String>());
    // create merged service
    mergeService = new MergedStreamService(null, cluster, cluster, null,
        new FSCheckpointProvider(cluster.getCheckpointDir()),
        new HashSet<String>());
    // create data
    createValidData();

    //expectedConsumePaths
    expectedConsumePaths.add("file:/tmp/com.inmobi.databus.distcp" +
        ".TestDistCPBaseService/system/mirrors/databusCluster/file-with-valid-data");
    expectedConsumePaths.add("file:/tmp/com.inmobi.databus.distcp" +
        ".TestDistCPBaseService/system/mirrors/databusCluster/file-with-junk-data");
    expectedConsumePaths.add("file:/tmp/com.inmobi.databus.distcp" +
        ".TestDistCPBaseService/system/mirrors/databusCluster/file1-empty");

  }
  @AfterTest
  public void cleanUP() throws IOException {
    // cleanup testRoot
    localFs.delete(testRoot, true);
    // cleaning up test Root1
    localFs.delete(testRoot1, true);
  }

  private void createInvalidData() throws IOException{
    localFs.mkdirs(testRoot);
    Path dataRoot = new Path(testRoot, mirrorService.getInputPath());
    localFs.mkdirs(dataRoot);
    //one empty file
    Path p = new Path(dataRoot, "file1-empty");
    localFs.create(p);

    //one file with data put invalid paths
    p = new Path(dataRoot, "file-with-junk-data");
    FSDataOutputStream out = localFs.create(p);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
    writer.write("junkfile-1\n");
    writer.write("junkfile-2\n");
    writer.close();

  }

  private void createValidData() throws IOException {
    Path dataRoot = new Path(testRoot, mirrorService.getInputPath());
    localFs.mkdirs(dataRoot);
    //create invalid data
    createInvalidData();

    //one valid & invalid data file
    Path data_file = new Path(testRoot, "data-file1");
    localFs.create(data_file);

    Path data_file1 = new Path(testRoot, "data-file2");
    localFs.create(data_file1);

    //one file with data and one valid path and one invalid path
    Path p = new Path(dataRoot, "file-with-valid-data");
    FSDataOutputStream  out = localFs.create(p);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
    writer.write(data_file.toString() +"\n");
    writer.write("some-junk-path\n");
    writer.write(data_file1.toString() + "\n");
    writer.close();
  }




  @Test(priority = 2)
  public void testNegative() throws Exception {
    cleanUP();
    createInvalidData();
    Map<String, FileStatus> fileCopyListMap = mirrorService
        .getDistCPInputFile();
    // since all data is invalid
    // output of this function should be null
    assert fileCopyListMap.size() == 0;

  }
  

  
   private void createDataWithDuplicateFileNames(DistcpBaseService service)
       throws IOException {
     Path dataRoot = new Path(testRoot, service.getInputPath());
     localFs.mkdirs(dataRoot);
     Path dataRoot1 = new Path(testRoot1, service.getInputPath());
     localFs.mkdirs(dataRoot1);
     // one valid & invalid data file
     Path data_file = new Path(testRoot, "data-file1");
     localFs.create(data_file);

     Path data_file1 = new Path(testRoot1, "data-file1");
     localFs.create(data_file1);

     // one file with data and one valid path and one invalid path
     Path p = new Path(dataRoot, "file-with-valid-data");
     FSDataOutputStream out = localFs.create(p);
     BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
     writer.write(data_file.toString() + "\n");
     writer.write("some-junk-path\n");
     writer.write(data_file1.toString() + "\n");
     writer.close();
   }

  public void testDuplicateFileNamesForMirrorService() throws Exception {

    cleanUP();
    createDataWithDuplicateFileNames(mirrorService);

    Map<String, FileStatus> fileCopyList = mirrorService.getDistCPInputFile();
    // assert that both the paths are present
    assert (fileCopyList.size() == 2);
    assert fileCopyList.values().contains(
        localFs.getFileStatus(new Path(expectedFileName1)));
    assert fileCopyList.values().contains(
        localFs.getFileStatus(new Path(expectedFileName3)));
  }


}
