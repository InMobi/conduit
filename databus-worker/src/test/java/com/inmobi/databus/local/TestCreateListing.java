package com.inmobi.databus.local;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
import com.inmobi.databus.FSCheckpointProvider;


public class TestCreateListing {


  private static Logger LOG = Logger.getLogger(TestCreateListing.class);
  Path rootDir = new Path
  ("/tmp/test-databus/databus/");

  FileSystem localFs;

  @BeforeTest
  private void setUP() throws Exception{
    localFs = FileSystem.getLocal(new Configuration());
    // clean up the test data if any thing is left in the previous runs
    cleanup();
  }

  @AfterTest
  private void cleanup() throws Exception{
    localFs.delete(rootDir, true);

  }

  @Test
  public void testCreateListing1() throws Exception{

    List<String> streamsToProcess = new ArrayList<String>();
    streamsToProcess.add("stream1");
    Path collectorPath = new Path(rootDir, "data/stream1/collector1");
    localFs.mkdirs(collectorPath);
    Map<String, String> clusterConf = new HashMap<String, String>();
    clusterConf.put("hdfsurl", localFs.getUri().toString());
    clusterConf.put("jturl", "local");
    clusterConf.put("name", "databusCluster");
    clusterConf.put("jobqueuename", "default");
    Set<String> sourceNames = new HashSet<String>();
    sourceNames.add("stream1");
    Cluster cluster = new Cluster(clusterConf, rootDir.toString(), null, sourceNames);

    Map<FileStatus, String> results = new HashMap<FileStatus, String>();
    Set<FileStatus> trashSet = new HashSet<FileStatus>();
    Map<String, FileStatus> checkpointPaths = new HashMap<String,
    FileStatus>();

    LocalStreamService service = new LocalStreamService(null, cluster, null,
        new FSCheckpointProvider("/tmp/test-databus/databus/checkpoint"),
        streamsToProcess);
    service.createListing(localFs, localFs.getFileStatus(new Path(rootDir,
    "data")), results, trashSet, checkpointPaths);

    assert results.size() == 0;
    assert trashSet.size() == 0;
    assert checkpointPaths.size() == 0;

    //create only current file and scribe_stats
    localFs.create(new Path(collectorPath, "stream1_current"));
    localFs.create(new Path(collectorPath, "scribe_stats"));

    service.createListing(localFs, localFs.getFileStatus(new Path(rootDir,
    "data")), results, trashSet, checkpointPaths);

    assert results.size() == 0;
    assert trashSet.size() == 0;
    assert checkpointPaths.size() == 0;

    //create one data file
    Path dataFile = new Path(collectorPath, "datafile1");
    localFs.create(dataFile);
    service.createListing(localFs, localFs.getFileStatus(new Path(rootDir,
    "data")), results, trashSet, checkpointPaths);

    assert results.size() == 0;
    assert trashSet.size() == 0;
    assert checkpointPaths.size() == 0;

    service.createListing(localFs, localFs.getFileStatus(new Path(rootDir,
 "data")), results, trashSet,
        checkpointPaths);

    // 0 bytes file found should not be deleted as that is the current file
    assert results.size() == 0;
    assert trashSet.size() == 0;
    assert checkpointPaths.size() == 0;
    assert localFs.exists(dataFile);
    Thread.sleep(1000);
    Path datafile2 = new Path(collectorPath, "datafile2");
    FSDataOutputStream out = localFs.create(datafile2);
    out.writeBytes("this is a testcase");
    out.close();

    service.createListing(localFs, localFs.getFileStatus(new Path(rootDir,
 "data")), results, trashSet,
        checkpointPaths);

    // there is no next file hence this should not be processed nor deleted
    // previous 0 byte file should be deleted
    assert results.size() == 0;
    assert trashSet.size() == 0;
    assert checkpointPaths.size() == 0;
    assert localFs.exists(datafile2);
    assert !localFs.exists(dataFile);
    Thread.sleep(1000);
     out = localFs.create(new Path(collectorPath,
    "datafile3"));
    out.writeBytes("this is a testcase");
    out.close();


    clearLists(results, trashSet, checkpointPaths);
    service.createListing(localFs, localFs.getFileStatus(new Path(rootDir,
    "data")), results, trashSet, checkpointPaths);
    for (FileStatus file : results.keySet())  {
       LOG.info("File Name [" + file.getPath() + "]");
    }
    assert results.size() == 1;
    assert trashSet.size() == 0;
    assert checkpointPaths.size() == 1;

    Thread.sleep(1000);
    out = localFs.create(new Path(collectorPath,
    "datafile4"));
    out.writeBytes("this is a testcase");
    out.close();
    out = localFs.create(new Path(collectorPath,
    "datafile5"));
    Thread.sleep(1000);
    out.writeBytes("this is a testcase");
    out.close();
    Thread.sleep(1000);
    out = localFs.create(new Path(collectorPath,
    "datafile6"));
    out.writeBytes("this is a testcase");
    out.close();
    Thread.sleep(1000);
    out = localFs.create(new Path(collectorPath,
    "datafile7"));
    out.writeBytes("this is a testcase");
    out.close();

    clearLists(results, trashSet, checkpointPaths);
    service.createListing(localFs, localFs.getFileStatus(new Path(rootDir,
    "data")), results, trashSet, checkpointPaths);
    for (FileStatus file : results.keySet())  {
      LOG.info("File Name [" + file.getPath() + "]");
    }

    assert results.size() == 5;
    assert trashSet.size() == 0;
    assert checkpointPaths.size() == 1;
    Thread.sleep(1000);
    out = localFs.create(new Path(collectorPath,
    "datafile8"));
    out.writeBytes("this is a testcase");
    out.close();
    Thread.sleep(1000);
    out = localFs.create(new Path(collectorPath,
    "datafile9"));
    out.writeBytes("this is a testcase");
    out.close();

    clearLists(results, trashSet, checkpointPaths);
    service.createListing(localFs, localFs.getFileStatus(new Path(rootDir,
    "data")), results, trashSet, checkpointPaths);
    for (FileStatus file : results.keySet())  {
      LOG.info("File Name [" + file.getPath() + "]");
    }
    assert results.size() == 7;
    assert trashSet.size() == 1;
    assert checkpointPaths.size() == 1;

    // create a 0 byte file
    Thread.sleep(1000);// modification time on local fs is reported in
                       // seconds,so to have diff modification time sleep for 1
                       // s
    localFs.create(new Path(collectorPath, "datafile10"));
    clearLists(results, trashSet, checkpointPaths);
    service.createListing(localFs,
        localFs.getFileStatus(new Path(rootDir, "data")), results, trashSet,
        checkpointPaths);
    assert results.size() == 8;
    assert trashSet.size() == 2;
    assert checkpointPaths.size() == 1;

  }

  public void clearLists(Map<FileStatus, String> results,
  Set<FileStatus> trashSet, Map<String, FileStatus> checkpointPaths) {
    results.clear();
    trashSet.clear();
    checkpointPaths.clear();
  }
}
