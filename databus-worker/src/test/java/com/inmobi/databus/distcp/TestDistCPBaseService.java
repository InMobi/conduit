package com.inmobi.databus.distcp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.DatabusConfigParser;
import com.inmobi.databus.DestinationStream;


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
    mirrorService = new MirrorStreamService(null, cluster, cluster, null);
    // create merged service
    mergeService = new MergedStreamService(null, cluster, cluster, null);
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

  @Test(priority = 1)
  public void testPositive() throws Exception {
    Map<Path, FileSystem> consumePaths = new HashMap<Path, FileSystem>();
    List<String> result=new ArrayList<String>();
    String currentLine = "";
    Path p = mirrorService.getDistCPInputFile(consumePaths, testRoot);
    LOG.info("distcp input [" + p + "]");
    FSDataInputStream in = localFs.open(p);
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    while ((currentLine = reader.readLine()) != null) {
      result.add(currentLine);
    }
    reader.close();
    // assert that the minuteFileName inside the valid file with data
    // matches our expectedFileName1
    assert result.contains(expectedFileName1);
    assert result.contains(expectedFileName2);

    //second line was junkpath which would be skipped instead the next valid
    // path in input should be present

    Set<String> resultSet = new HashSet<String>();
    //compare consumePaths with expectedOutput
    for (Path consumePath : consumePaths.keySet()) {
      //cant compare the path generated using timestamp
      //The final path on destinationCluster which contains all valid
      // minutefileNames has a suffix of timestamp to it
      if (!consumePath.toString().contains("file:/tmp/com.inmobi.databus" +
          ".distcp.TestDistCPBaseService/databusCluster")) {
        LOG.info("Path to consume [" + consumePath + "]");
        resultSet.add(consumePath.toString());
      }
    }
    assert resultSet.containsAll(expectedConsumePaths);
    assert consumePaths.size() == resultSet.size() + 1;
  }


  @Test(priority = 2)
  public void testNegative() throws Exception {
    cleanUP();
    createInvalidData();
    Map<Path, FileSystem> consumePaths = new HashMap<Path, FileSystem>();
    Path p = mirrorService.getDistCPInputFile(consumePaths, testRoot);
    // since all data is invalid
    // output of this function should be null
    assert p == null;

  }
  
  @Test
  public void testSplitFileName() throws Exception {
    Set<String> streamsSet = new HashSet<String>();
    streamsSet.add("test-stream");
    streamsSet.add("test_stream");
    streamsSet.add("test_streams");
    streamsSet.add("test_stream_2");
    // file name in which collector name has hyphen
    String fileName1 = "databus-test-test_stream-2012-11-27-21-20_00000.gz";
    // file name in which stream name has hyphen
    String fileName2 = "databus_test-test-stream-2012-11-27-21-20_00000.gz";
    // file name in which stream name is subset of another stream name in the
    // streamsSet
    String fileName3 = "databus_test-test_streams-2012-11-27-21-20_00000.gz";
    String fileName4 = "databus_test-test_stream_2-2012-11-27-21-20_00000.gz";
    // file name in which stream name is not in streamsSet passed
    String fileName5 = "databus_test-test_stream-2-2012-11-27-21-20_00000.gz";
    // timestamp part of the filename has wrong format
    String fileName6 = "databus_test-test_stream-2-2012-11-27-21-20-00000.gz";
    String fileName7 = "databus_test-test_stream-2-2012-11_27-21-20_00000.gz";
    // get stream names from file name
    String expectedStreamName1 = MergedStreamService.getCategoryFromFileName(
        fileName1, streamsSet);
    String expectedStreamName2 = MergedStreamService.getCategoryFromFileName(
        fileName2, streamsSet);
    String expectedStreamName3 = MergedStreamService.getCategoryFromFileName(
        fileName3, streamsSet);
    String expectedStreamName4 = MergedStreamService.getCategoryFromFileName(
        fileName4, streamsSet);
    String expectedStreamName5 = MergedStreamService.getCategoryFromFileName(
        fileName5, streamsSet);
    String expectedStreamName6 = MergedStreamService.getCategoryFromFileName(
        fileName6, streamsSet);
    String expectedStreamName7 = MergedStreamService.getCategoryFromFileName(
        fileName7, streamsSet);
    assert expectedStreamName1.compareTo("test_stream") == 0;
    assert expectedStreamName2.compareTo("test-stream") == 0;
    assert expectedStreamName3.compareTo("test_streams") == 0;
    assert expectedStreamName4.compareTo("test_stream_2") == 0;
    assert expectedStreamName5 == null;
    assert expectedStreamName6 == null;
    assert expectedStreamName7 == null;
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
    Map<Path, FileSystem> consumePaths = new HashMap<Path, FileSystem>();
    Path p = mirrorService.getDistCPInputFile(consumePaths, testRoot);
    LOG.info("distcp input [" + p + "]");
    FSDataInputStream in = localFs.open(p);
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    String result;
    Set<String> resultSet = new HashSet<String>();
    while ((result = reader.readLine()) != null) {
      resultSet.add(result);
    }
    // assert that both the paths are present
    assert (resultSet.size() == 2);
    assert resultSet.contains(expectedFileName1);
    assert resultSet.contains(expectedFileName3);
  }

  @Test(priority = 4)
  public void testDuplicateFileNamesForMergeService() throws Exception {

    cleanUP();
    createDataWithDuplicateFileNames(mergeService);
    Map<Path, FileSystem> consumePaths = new HashMap<Path, FileSystem>();
    Path p = mergeService.getDistCPInputFile(consumePaths, testRoot);
    LOG.info("distcp input [" + p + "]");
    FSDataInputStream in = localFs.open(p);
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    String result;
    Set<String> resultSet = new HashSet<String>();
    while ((result = reader.readLine()) != null) {
      resultSet.add(result);
    }
    // assert that both the paths are present
    assert (resultSet.size() == 1);
    assert (resultSet.contains(expectedFileName1) || resultSet
        .contains(expectedFileName3));
  }

  private void writeToConsumerFile(FSDataOutputStream out, List<Path> paths)
      throws IOException {
    try {
    for (Path p : paths) {
      out.writeBytes(p.toString());
      out.writeBytes("\n");
    }
    } finally {
      out.close();
    }
  }

  private DatabusConfig setUPForYTMPaths() throws Exception {
    DatabusConfigParser parser = new DatabusConfigParser(
        "testDatabusService_simple.xml");
    DatabusConfig config = parser.getConfig();
    Cluster srcCluster = config.getClusters().get("testcluster1");
    Cluster destinationCluster = config.getClusters().get("testcluster2");
    Cluster currentCluster = null;
    mergedService1 = new MergedStreamService(config, srcCluster,
        destinationCluster, currentCluster);
    srcFs = FileSystem.get(new URI(srcCluster.getHdfsUrl()),
        srcCluster.getHadoopConf());
    return config;
  }

  @Test(priority = 5)
  public void testYetToBeMovedPaths() throws Exception {

    DatabusConfig config = setUPForYTMPaths();
    Cluster srcCluster = config.getClusters().get("testcluster1");
    try {
    Cluster destinationCluster = config.getClusters().get("testcluster2");
    List<Path> paths = new ArrayList<Path>();
    Date currntdate = new Date();
    Path localPath = new Path(srcCluster.getLocalDestDir("test1", currntdate));
    Path dataFile1 = new Path(localPath, "dataFile1");
    Path dataFile2 = new Path(localPath, "dataFile2");
    srcFs.create(dataFile1);
    paths.add(dataFile1);

    srcFs.create(dataFile2);
    paths.add(dataFile2);

    Calendar calendar = Calendar.getInstance();
    calendar.setTime(currntdate);
    calendar.add(Calendar.MINUTE, 1);
    Path localPath1 = new Path(srcCluster.getLocalDestDir("test1",
        calendar.getTime()));
    Path invalidFile = new Path(localPath1, "missingFile");// dont create this
                                                           // file but just add
                                                           // to consumers
    paths.add(invalidFile);

    calendar.add(Calendar.MINUTE, 1);
    Path localPath2 = new Path(srcCluster.getLocalDestDir("test1",
        calendar.getTime()));
    Path nextMinFile = new Path(localPath2, "dataFile3");
    Path yetToBeMoved = new Path(localPath2, "ytm");
    srcFs.create(nextMinFile);
    paths.add(nextMinFile);
    paths.add(yetToBeMoved);

    // Prepare a consumer file on srcFs
    Path consumerFile = srcCluster.getConsumePath(destinationCluster);
    FSDataOutputStream out = srcFs.create(consumerFile);
    writeToConsumerFile(out, paths);

    Set<String> minFilesSet = new HashSet<String>();
    Set<Path> yetToBeMovedPaths = new HashSet<Path>();
    mergedService1
        .readConsumePath(srcFs, consumerFile, minFilesSet, yetToBeMovedPaths);
    assert (yetToBeMovedPaths.contains(yetToBeMoved));
    assert (minFilesSet.contains(dataFile1.toString()));
    assert (minFilesSet.contains(dataFile2.toString()));
    assert (minFilesSet.contains(nextMinFile.toString()));
    } finally {
    // cleanup
      srcFs.delete(new Path(srcCluster.getRootDir()), true);
    }
  }

  @Test(priority = 6)
  public void testWriteYetToBeMovedFile() throws Exception {
    DatabusConfig config = setUPForYTMPaths();
    Cluster srcCluster = config.getClusters().get("testcluster1");
    Cluster dstnCluster = config.getClusters().get("testcluster2");
    Path ytm1 = new Path(srcCluster.getLocalDestDir("test1", new Date()),
        "ytm1");
    Path ytm2 = new Path(srcCluster.getLocalDestDir("test1", new Date()),
        "ytm2");
    System.out.println("YYM " + ytm1);
    Set<Path> yetToBeMovedPaths = new HashSet<Path>();
    yetToBeMovedPaths.add(ytm1);
    yetToBeMovedPaths.add(ytm2);
    try {
    mergedService1.writeYetToBeMovedFile(srcCluster.getTmpPath(),
        yetToBeMovedPaths);
    Path consumerDir = srcCluster.getConsumePath(dstnCluster);
    FileStatus[] status = srcFs.listStatus(consumerDir);// only ytm consumer
                                                        // file would be there
    FSDataInputStream fsin = srcFs.open(status[0].getPath());
    BufferedReader in = new BufferedReader(new InputStreamReader(fsin));
    Set<String> ytmPaths = new HashSet<String>();
    String line;
    while ((line = in.readLine()) != null) {
      ytmPaths.add(line);

    }
    assert ytmPaths.contains(ytm1.toString());
    assert ytmPaths.contains(ytm2.toString());
    } finally {
      srcFs.delete(new Path(srcCluster.getRootDir()), true);
    }

  }

}
