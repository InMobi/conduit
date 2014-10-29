package com.inmobi.conduit.local;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.inmobi.conduit.Cluster;
import com.inmobi.conduit.FSCheckpointProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;


public class TestCreateListing {


  private static Logger LOG = Logger.getLogger(TestCreateListing.class);
  Path rootDir = new Path
  ("/tmp/test-conduit/conduit/");

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

    Set<String> streamsToProcess = new HashSet<String>();
    streamsToProcess.add("stream1");
    Path collectorPath = new Path(rootDir, "data/stream1/collector1");
    localFs.mkdirs(collectorPath);
    Map<String, String> clusterConf = new HashMap<String, String>();
    clusterConf.put("hdfsurl", localFs.getUri().toString());
    clusterConf.put("jturl", "local");
    clusterConf.put("name", "conduitCluster");
    clusterConf.put("jobqueuename", "default");
    Set<String> sourceNames = new HashSet<String>();
    sourceNames.add("stream1");
    Cluster cluster = new Cluster(clusterConf, rootDir.toString(), null, sourceNames);

    Map<FileStatus, String> results = new HashMap<FileStatus, String>();
    Set<FileStatus> trashSet = new HashSet<FileStatus>();
    Table<String, String, String> checkpointPaths = HashBasedTable.create();

    LocalStreamService service = new LocalStreamService(null, cluster, null,
        new FSCheckpointProvider("/tmp/test-conduit/conduit/checkpoint"),
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
    FSDataOutputStream out1 = localFs.create(new Path(collectorPath, "datafile1"));
    out1.close();
    service.createListing(localFs, localFs.getFileStatus(new Path(rootDir,
    "data")), results, trashSet, checkpointPaths);

    Thread.sleep(2000);
    assert results.size() == 0;
    assert trashSet.size() == 0;
    assert checkpointPaths.size() == 0;

    //sleep for 10 msec
    Thread.sleep(10);
    service.createListing(localFs, localFs.getFileStatus(new Path(rootDir,
 "data")), results, trashSet,
        checkpointPaths);

    //0 bytes file found should be deleted and not current file
    assert results.size() == 0;
    assert trashSet.size() == 0;
    assert checkpointPaths.size() == 0;


    FSDataOutputStream out = localFs.create(new Path(collectorPath,
    "datafile2"));
    out.writeBytes("this is a testcase");
    out.close();

    //sleep for 1sec
    Thread.sleep(1000);
    service.createListing(localFs, localFs.getFileStatus(new Path(rootDir,
        "data")), results, trashSet, checkpointPaths);

    // only 1 file exist at this point hence won't be picked
    assert results.size() == 1;
    assert trashSet.size() == 0;
    assert checkpointPaths.size() == 1;

    out = localFs.create(new Path(collectorPath, "datafile3"));
    out.writeBytes("this is a testcase");
    out.close();

    //don't sleep now
    clearLists(results, trashSet, checkpointPaths);
    service.createListing(localFs, localFs.getFileStatus(new Path(rootDir,
        "data")), results, trashSet, checkpointPaths);
    for (FileStatus file : results.keySet())  {
       LOG.info("File Name [" + file.getPath() + "]");
    }
    assert results.size() == 2;
    assert trashSet.size() == 0;
    assert checkpointPaths.size() == 1;

    Thread.sleep(1000);

    out = localFs.create(new Path(collectorPath,
    "datafile4"));
    out.writeBytes("this is a testcase");
    out.close();
    out = localFs.create(new Path(collectorPath,
    "datafile5"));
    out.writeBytes("this is a testcase");
    out.close();
    out = localFs.create(new Path(collectorPath,
    "datafile6"));
    out.writeBytes("this is a testcase");
    out.close();
    // sleep 1sec before creating one file to have diff modification time
    // from other files on local file system
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

    assert results.size() == 6;
    assert trashSet.size() == 0;
    assert checkpointPaths.size() == 1;

    out = localFs.create(new Path(collectorPath, "datafile8"));
    out.writeBytes("this is a testcase");
    out.close();

    Thread.sleep(1000);
    out = localFs.create(new Path(collectorPath, "datafile9"));
    out.writeBytes("this is a testcase");
    out.close();

    clearLists(results, trashSet, checkpointPaths);
    service.createListing(localFs, localFs.getFileStatus(new Path(rootDir,
    "data")), results, trashSet, checkpointPaths);
    for (FileStatus file : results.keySet())  {
      LOG.info("File Name [" + file.getPath() + "]");
    }
    assert results.size() == 8;
    assert trashSet.size() == 2;
    assert checkpointPaths.size() == 1;

  }

  public void clearLists(Map<FileStatus, String> results,
  Set<FileStatus> trashSet, Table<String, String, String> checkpointPaths) {
    results.clear();
    trashSet.clear();
    checkpointPaths.clear();
  }
}
