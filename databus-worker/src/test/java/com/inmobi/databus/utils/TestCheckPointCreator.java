package com.inmobi.databus.utils;

import java.util.Date;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.inmobi.databus.CheckpointProvider;
import com.inmobi.databus.Cluster;
import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.DatabusConfigParser;
import com.inmobi.databus.FSCheckpointProvider;

public class TestCheckPointCreator {
  public static final String CLUSTER1 = "testcluster1";
  public static final String CLUSTER2 = "testcluster2";
  public static final String STREAM1 = "stream1";
  public static final String STREAM2 = "stream2";
  private DatabusConfigParser configParser;
  private DatabusConfig config;
  private Cluster cluster1, cluster2;
  private FileSystem fs1, fs2;

  @BeforeMethod
  public void setup() throws Exception {
    configParser = new DatabusConfigParser("test-mss-databus1.xml");
    config = configParser.getConfig();
    cluster1 = config.getClusters().get(CLUSTER1);
    fs1 = FileSystem.get(cluster1.getHadoopConf());
    cluster2 = config.getClusters().get(CLUSTER2);
    fs2 = FileSystem.get(cluster2.getHadoopConf());
    fs1.delete(new Path(cluster1.getRootDir()), true);
    fs2.delete(new Path(cluster2.getRootDir()), true);
  }
  @Test
  public void testCheckPointMergeNoSourceProvided() throws Exception {
    Date date = new Date();
    Path p1 = new Path(cluster1.getLocalDestDir(STREAM1, date.getTime()));
    Path p2 = new Path(cluster2.getLocalDestDir(STREAM1, date.getTime()));
    fs1.create(p1);
    fs2.create(p2);
    CheckPointCreator creator = new CheckPointCreator(config, null, CLUSTER2,
        STREAM1, date);
    creator.createCheckPoint();
    String ck1 = "MergedStreamService" + CLUSTER1 + STREAM1;
    String ck2 = "MergedStreamService" + CLUSTER2 + STREAM1;
    assert (fs2.exists(new Path(cluster2.getCheckpointDir(),
 ck1 + ".ck")));
    assert (fs2.exists(new Path(cluster2.getCheckpointDir(),
 ck2 + ".ck")));
    CheckpointProvider provider = new FSCheckpointProvider(
        cluster2.getCheckpointDir());
    assert (new Path(new String(provider.read(ck1))).equals(p1));
    assert (new Path(new String(provider.read(ck2))).equals(p2));
  }

  @Test
  public void testCheckPointMirrorNoSourceProvided() throws Exception {
    Date date = new Date();
    Path p1 = new Path(cluster1.getFinalDestDir(STREAM2, date.getTime()));
    fs1.create(p1);
    CheckPointCreator creator = new CheckPointCreator(config, null, CLUSTER2,
        STREAM2, date);
    creator.createCheckPoint();
    String ck1 = "MirrorStreamService" + CLUSTER1 + STREAM2;
    String ck2 = "MirrorStreamService" + CLUSTER2 + STREAM2;
    assert (fs2.exists(new Path(cluster2.getCheckpointDir(), ck1 + ".ck")));
    assert (!fs2.exists(new Path(cluster2.getCheckpointDir(), ck2 + ".ck")));
    CheckpointProvider provider = new FSCheckpointProvider(
        cluster2.getCheckpointDir());
    assert (new Path(new String(provider.read(ck1))).equals(p1));
  }

  @Test
  public void testCheckPointMergeSourceProvided() throws Exception {
    Date date = new Date();
    Path p1 = new Path(cluster1.getLocalDestDir(STREAM1, date.getTime()));
    Path p2 = new Path(cluster2.getLocalDestDir(STREAM1, date.getTime()));
    System.out.println("creating path " + p1);
    fs1.create(p1);
    fs2.create(p2);
    CheckPointCreator creator = new CheckPointCreator(config, CLUSTER1,
        CLUSTER2, STREAM1, date);
    creator.createCheckPoint();
    String ck1 = "MergedStreamService" + CLUSTER1 + STREAM1;
    String ck2 = "MergedStreamService" + CLUSTER2 + STREAM1;
    assert (fs2.exists(new Path(cluster2.getCheckpointDir(), ck1 + ".ck")));
    assert (!fs2.exists(new Path(cluster2.getCheckpointDir(), ck2 + ".ck")));
    CheckpointProvider provider = new FSCheckpointProvider(
        cluster2.getCheckpointDir());
    assert (new Path(new String(provider.read(ck1))).equals(p1));
  }

  @Test
  public void testCheckPointMirrorSourceProvided() throws Exception {
    Date date = new Date();
    Path p1 = new Path(cluster1.getFinalDestDir(STREAM2, date.getTime()));
    fs1.create(p1);
    CheckPointCreator creator = new CheckPointCreator(config, CLUSTER1,
        CLUSTER2, STREAM2, date);
    creator.createCheckPoint();
    String ck1 = "MirrorStreamService" + CLUSTER1 + STREAM2;
    String ck2 = "MirrorStreamService" + CLUSTER2 + STREAM2;
    assert (fs2.exists(new Path(cluster2.getCheckpointDir(), ck1 + ".ck")));
    assert (!fs2.exists(new Path(cluster2.getCheckpointDir(), ck2 + ".ck")));
    CheckpointProvider provider = new FSCheckpointProvider(
        cluster2.getCheckpointDir());
    assert (new Path(new String(provider.read(ck1))).equals(p1));
  }

  @Test(expectedExceptions = { Exception.class })
  public void testCheckPointMirrorCheckPointPathDoesntExist() throws Exception {
    Date date = new Date();
    CheckPointCreator creator = new CheckPointCreator(config, null, CLUSTER2,
        STREAM2, date);
    creator.createCheckPoint();
  }
}
